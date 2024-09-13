import polars as pl

from base.metrics_trend import MetricsTrendType
from calculations.utils import PERIOD_WINDOW_SIZE


def build_rolling_mean_agg_expr(
    column_name: str,
    metric_trend_type: MetricsTrendType,
    sub_group: bool = False,
):
    if not sub_group:
        return pl.col(column_name).rolling_mean(
            window_size=PERIOD_WINDOW_SIZE[metric_trend_type]
        )
    return (
        pl.col(column_name)
        .rolling_mean(window_size=PERIOD_WINDOW_SIZE[metric_trend_type])
        .over("sub_group")
    )


def build_rolling_stddev_agg_expr(
    column_name: str,
    metric_trend_type: MetricsTrendType,
    sub_group: bool = False,
):
    if not sub_group:
        return pl.col(column_name).rolling_std(
            window_size=PERIOD_WINDOW_SIZE[metric_trend_type]
        )
    return (
        pl.col(column_name)
        .rolling_std(window_size=PERIOD_WINDOW_SIZE[metric_trend_type])
        .over("sub_group")
    )


def build_pct_change_agg_expr(
    column_name: str, diff_col_name: str, sub_group: bool = False
):
    if not sub_group:
        return pl.col(column_name).pct_change().alias(diff_col_name)  # .fill_null(0)
    else:
        return (
            pl.col(column_name).pct_change().over("sub_group").alias(diff_col_name)
        )  # .fill_null(0)


def build_z_score_agg_expr(column_name, mean_col, stddev_col):
    return (
        pl.when((pl.col(stddev_col) == 0) | pl.col(stddev_col).is_null())
        .then(0)
        .otherwise((pl.col(column_name) - pl.col(mean_col)) / pl.col(stddev_col))
    )
