import polars as pl

from base.metrics import Metric
from base.metrics_trend import MetricsTrendType
from calculations.anomaly.utils import (
    build_pct_change_agg_expr,
    build_rolling_mean_agg_expr,
    build_rolling_stddev_agg_expr,
    build_z_score_agg_expr,
)
from calculations.utils import PERIOD_WINDOW_SIZE


def calculate_polars_anomaly(
    trend_df: pl.DataFrame,
    metric_name: str,
    metric_trend_type: MetricsTrendType,
    sub_group: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Find anomalies using zscore

    """

    # calculate rolling mean and rolling stddev
    rolling_mean_agg_expr = build_rolling_mean_agg_expr(
        metric_name, metric_trend_type, sub_group
    )
    rolling_stddev_agg_expr = build_rolling_stddev_agg_expr(
        metric_name, metric_trend_type, sub_group
    )

    mean_col = "rolling_mean"
    stddev_col = "rolling_stddev"
    z_score_col = "z_score"
    trend_df = trend_df.with_columns(
        [
            rolling_mean_agg_expr.alias(mean_col),
            rolling_stddev_agg_expr.alias(stddev_col),
        ]
    )

    # calculate z score
    z_score_agg_expr = build_z_score_agg_expr(metric_name, mean_col, stddev_col)
    trend_df = trend_df.with_columns(z_score_agg_expr.alias(z_score_col))

    return trend_df, z_score_col


def calculate_polars_rowdiff_anomaly(
    trend_df: pl.DataFrame,
    metric_name: str,
    metric_trend_type: str,
    sub_group: bool = False,
):
    # first sort the dataframe on the DateLabel, but since it is string, parse it back to date
    # trend_df = get_date_from_date_label(trend_df, metric_trend_type)

    diff_col_name = f"{metric_name}_diff"
    pct_change_agg_expr = build_pct_change_agg_expr(
        metric_name, diff_col_name, sub_group
    )
    trend_df = trend_df.with_columns(pct_change_agg_expr)

    # handle infinite values in case pct change is inf
    trend_df = trend_df.with_columns(
        pl.when(pl.col(diff_col_name).is_infinite())
        .then(None)
        .otherwise(pl.col(diff_col_name))
        .keep_name()
    )

    # calculate rolling mean and rolling stddev for previous period difference
    rolling_mean_agg_expr = build_rolling_mean_agg_expr(
        diff_col_name, metric_trend_type, sub_group
    )
    rolling_stddev_agg_expr = build_rolling_stddev_agg_expr(
        diff_col_name, metric_trend_type, sub_group
    )

    mean_col = "rolling_mean_diff"
    stddev_col = "rolling_stddev_diff"
    z_score_col = "z_score_diff"
    trend_df = trend_df.with_columns(
        [
            rolling_mean_agg_expr.alias(mean_col),
            rolling_stddev_agg_expr.alias(stddev_col),
        ]
    )

    # calculate z-score
    z_score_agg_expr = build_z_score_agg_expr(diff_col_name, mean_col, stddev_col)
    trend_df = trend_df.with_columns(z_score_agg_expr.alias(z_score_col))

    return trend_df, z_score_col


def calculate_polars_yoy_diff_anomaly(
    trend_df: pl.DataFrame,
    metric_name: str,
    metric_trend_type: str,
    sub_group: bool = False,
):
    trend_df = trend_df.with_columns(
        pl.col("DateLabel")
        .apply(lambda x: f"{int(x[:4])-1}{x[4:]}")
        .alias("DateLabelYoY")
    )

    select_cols = ["DateLabel", metric_name]
    left_on = ["DateLabelYoY"]
    right_on = ["DateLabel"]
    if sub_group:
        select_cols.append("sub_group")
        left_on.append("sub_group")
        right_on.append("sub_group")

    trend_df = trend_df.join(
        trend_df.select(select_cols),
        how="left",
        left_on=left_on,
        right_on=right_on,
        suffix="_YoY",
    )

    yoy_col_name = f"{metric_name}_YoY_diff"
    mean_col = "rolling_mean_YoY_diff"
    stddev_col = "rolling_stddev_YoY_diff"
    z_score_col = "z_score_YoY_diff"

    trend_df = trend_df.with_columns(
        pl.when(
            (pl.col(f"{metric_name}_YoY") == 0)
            | pl.col(metric_name).is_null()
            | pl.col(f"{metric_name}_YoY").is_null()
        )
        .then(None)
        .otherwise(
            (pl.col(metric_name) - pl.col(f"{metric_name}_YoY"))
            / pl.col(f"{metric_name}_YoY")
        )
        .alias(yoy_col_name)
    )

    # calculate rolling mean and rolling stddev for previous period difference
    rolling_mean_agg_expr = build_rolling_mean_agg_expr(
        yoy_col_name, metric_trend_type, sub_group
    )
    rolling_stddev_agg_expr = build_rolling_stddev_agg_expr(
        yoy_col_name, metric_trend_type, sub_group
    )

    trend_df = trend_df.with_columns(
        [
            rolling_mean_agg_expr.alias(mean_col),
            rolling_stddev_agg_expr.alias(stddev_col),
        ]
    )

    # calculate z-score
    z_score_agg_expr = build_z_score_agg_expr(yoy_col_name, mean_col, stddev_col)
    trend_df = trend_df.with_columns(z_score_agg_expr.alias(z_score_col))

    return trend_df, z_score_col


def tag_polars_anomalies(
    column_name: str, df: pl.DataFrame, threshold: float, alias: str = "is_anomalous"
) -> pl.DataFrame:
    """
    Add a new boolean column to the dataframe, tagging z-score values beyond threshold as true
    """
    df = df.with_columns(
        (
            (pl.col(column_name) > pl.lit(threshold))
            | (pl.col(column_name) < pl.lit(-threshold))
        ).alias(alias)
    )

    return df


def calculate_databricks_anomaly(**kwargs):
    kwargs = None
    return kwargs


def calculate_databricks_rowdiff_anomaly(**kwargs):
    kwargs = None
    return kwargs


def calculate_databricks_yoy_diff_anomaly(**kwargs):
    kwargs = None
    return kwargs


def tag_databricks_anomalies(**kwargs):
    kwargs = None
    return kwargs
