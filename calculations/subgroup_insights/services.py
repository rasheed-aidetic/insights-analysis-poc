import itertools

import polars as pl
from base.general import ProcessingType
from calculations.subgroup_insights import repository as subgroup_trend_repository
from joblib import Parallel, delayed


def build_dimension_combinations(dimensions, rca_depth: int = None):
    # taking combinations of length 1, 2 and 3 only
    all_combinations = [
        comb
        for r in range(1, rca_depth + 1)
        for comb in itertools.combinations(dimensions, r)
    ]
    return all_combinations


def apply_subgroup_trend_agg(
    all_combinations,
    trend_df,
    dataframe,
    agg_expr,
    processing_type,
    time_intervals,
    metric_details,
):
    num_cores = -1  # Set to the number of available cores, or a specific number
    if processing_type == ProcessingType.IN_MEMORY:
        results = Parallel(n_jobs=num_cores, backend="threading")(
            delayed(subgroup_trend_repository.apply_polar_subgroup_trend_agg)(
                combination, trend_df, dataframe, agg_expr, metric_details
            )
            for combination in all_combinations
        )
        merged_df = pl.concat(results)

    elif processing_type == ProcessingType.DATABRICKS:
        results = Parallel(n_jobs=num_cores, backend="threading")(
            delayed(subgroup_trend_repository.apply_databricks_subgroup_trend_agg)(
                combination, trend_df, dataframe, agg_expr
            )
            for combination in all_combinations
        )
        merged_df = results  # combine results into single results for all combinations
    return merged_df


def apply_filter_on_subgroup_impact_df(
    trend_df,
    dimension_value_pairs,
    processing_type: ProcessingType = "in_memory",
):
    """Apply filter on subgroup impact dataframe to extract data for required dimension_value_pairs."""
    if processing_type == ProcessingType.IN_MEMORY:
        return subgroup_trend_repository.apply_polar_filter_on_subgroup_impact_df(
            trend_df=trend_df, dimension_value_pairs=dimension_value_pairs
        )
    elif processing_type == ProcessingType.DATABRICKS:
        return subgroup_trend_repository.apply_databricks_filter_on_subgroup_impact_df(
            trend_df=trend_df, dimension_value_pairs=dimension_value_pairs
        )


def merge_segment_baseline_and_comparison_data(
    baseline_df,
    comparison_df,
    metric_name,
    processing_type: ProcessingType = "in_memory",
):
    """Merge baseline and comparison df for segment comparison
    and calculate absolute impact difference"""
    if processing_type == ProcessingType.IN_MEMORY:
        return (
            subgroup_trend_repository.merge_polars_segment_baseline_and_comparison_data(
                baseline_df=baseline_df,
                comparison_df=comparison_df,
                metric_name=metric_name,
            )
        )
    elif processing_type == ProcessingType.DATABRICKS:
        return (
            subgroup_trend_repository.merge_databricks_segment_baseline_and_comparison_data()
        )
