import json
from copy import deepcopy
from typing import List

import polars as pl
from base.insights import DimensionValuePair, MetricsInsight
from base.metrics import DualColumnMetric, SingleColumnMetric


def apply_polar_subgroup_trend_agg(
    combination, trend_df, dataframe, agg_expr, metric_details: MetricsInsight
):
    # Group by Date and combination
    subset_metric = dataframe.group_by(["DateLabel"] + list(combination)).agg(agg_expr)

    # Merge the overall_metric back to the subset_metric
    subset_metric = subset_metric.join(
        trend_df, on="DateLabel", how="inner", suffix="_overall"
    )

    subset_metric = subset_metric.with_columns(
        [
            pl.Series(
                [
                    json.dumps(item)
                    for item in subset_metric.select(list(combination)).to_dicts()
                ]
            ).alias("sub_group"),
        ]
    )

    # Remove 'combination' columns from subset_metric
    subset_metric = subset_metric.drop(list(combination))
    subset_metric = calculate_polar_subgroup_impact(subset_metric, metric_details)
    return subset_metric


def apply_databricks_subgroup_trend_agg(**kwargs):
    """Build databricks subroup trend agg

    Returns:
        pl.Dataframe: combination aggregated dataframe with period
    """
    kwargs = None
    kwargs = calculate_databricks_subgroup_impact(
        kwargs=kwargs
    )  # calculate impact on metrics
    return kwargs


def calculate_polar_subgroup_impact(trend_df, metric_details: MetricsInsight):
    """Calculate impact of subgroups over the overall KPI with polar

    Args:
        trend_df (_type_): Data set containing values of sub group metrics and over all metrics
        metric_details (MetricsInsight): Metrics details
    """
    if isinstance(metric_details.metrics, SingleColumnMetric):
        impact_df = trend_df.with_columns(
            (pl.col(metric_details.name)).alias("absolute_impact"),
        ).with_columns(
            (
                pl.col(metric_details.name) / pl.col(metric_details.name + "_overall")
            ).alias("relative_impact")
        )
        return impact_df
    elif isinstance(metric_details.metrics, DualColumnMetric):
        impact_df = trend_df.with_columns(
            (
                pl.col(metric_details.name + "_overall")
                - (
                    (
                        pl.col(
                            metric_details.metrics.numerator_metric.name + "_overall"
                        )
                        - pl.col(metric_details.metrics.numerator_metric.name)
                    )
                    / (
                        pl.col(
                            metric_details.metrics.denominator_metric.name + "_overall"
                        )
                        - pl.col(metric_details.metrics.denominator_metric.name)
                    )
                )
            ).alias("absolute_impact")
        )
        impact_df = impact_df.with_columns(
            (
                (pl.col("absolute_impact") / pl.col(metric_details.name + "_overall"))
            ).alias("relative_impact")
        )
        return impact_df


def calculate_databricks_subgroup_impact(**kwargs):
    """Calculate impact of subgroups over the overall KPI in databricks"""
    kwargs = None
    return kwargs


def apply_polar_filter_on_subgroup_impact_df(
    trend_df: pl.DataFrame, dimension_value_pairs: List[DimensionValuePair]
):
    """Apply filter on subgroup impact dataframe to extract data for required dimension_value_pairs.

    Args:
        trend_df (pl.DataFrame): bubgroup impact dataframe.
        subgroup_dimension_value_pair (List[DimensionValuePair]): List of dimension value pairs to filter the dataframe.
    """
    filtered_df = deepcopy(trend_df)
    # string value search
    search_string = """\"{dimension}\": \"{value}\""""

    # integer / bool value search
    search_string2 = """\"{dimension}\": {value}"""
    for dimension_value_pair in dimension_value_pairs:

        filtered_df = filtered_df.filter(
            pl.col("sub_group").str.contains(
                f"{search_string.format(**dimension_value_pair.model_dump())}|{search_string2.format(**dimension_value_pair.model_dump())}"
            )
        )

    filtered_df = filtered_df.filter(
        pl.col("sub_group").str.count_matches(":") == len(dimension_value_pairs)
    )

    return filtered_df


def apply_databricks_filter_on_subgroup_impact_df(**kwargs):
    """Apply filter on subgroup impact dataframe to extract data for required dimension_value_pairs."""
    kwargs = None
    return kwargs


def merge_polars_segment_baseline_and_comparison_data(
    baseline_df: pl.DataFrame, comparison_df: pl.DataFrame, metric_name: str
):
    """Merge baseline and comparison df for segment comparison
    and calculate absolute impact difference"""
    baseline_df = baseline_df.join(
        comparison_df, on="sub_group", how="outer_coalesce", suffix="_comparison"
    )

    rename_dict = {}
    for column in baseline_df.columns:
        if not column.endswith("_comparison") and not column == "sub_group":
            rename_dict[column] = f"{column}_baseline"

    baseline_df = baseline_df.rename(rename_dict)

    baseline_df = baseline_df.with_columns(
        [
            pl.col("DateLabel_baseline").fill_null(pl.col("DateLabel_baseline").max()),
            pl.col(f"{metric_name}_overall_baseline").fill_null(
                pl.col(f"{metric_name}_overall_baseline").max()
            ),
            pl.col("size_overall_baseline").fill_null(
                pl.col("size_overall_baseline").max()
            ),
            pl.col("absolute_impact_baseline").fill_null(0),
            pl.col("relative_impact_baseline").fill_null(0),
            pl.col("DateLabel_comparison").fill_null(
                pl.col("DateLabel_comparison").max()
            ),
            pl.col(f"{metric_name}_overall_comparison").fill_null(
                pl.col(f"{metric_name}_overall_comparison").max()
            ),
            pl.col("size_overall_comparison").fill_null(
                pl.col("size_overall_comparison").max()
            ),
            pl.col("absolute_impact_comparison").fill_null(0),
            pl.col("relative_impact_comparison").fill_null(0),
        ]
    )

    baseline_df = baseline_df.with_columns(
        (
            pl.col("absolute_impact_comparison") - pl.col("absolute_impact_baseline")
        ).alias("absolute_impact_diff")
    )

    return baseline_df


def merge_databricks_segment_baseline_and_comparison_data(**kwargs):
    """Apply filter on subgroup impact dataframe to extract data for required dimension_value_pairs."""
    kwargs = None
    return kwargs
