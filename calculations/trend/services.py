"""all model related functions for trend calculations"""

from typing import Any, Union

import calculations.utils as calculation_utils
import polars as pl
from base.general import AggregateMethod, Filter, ProcessingType
from base.insights import MetricsInsight
from base.metrics import DualColumnMetric, SingleColumnMetric
from calculations.trend import repository as trend_repository


def build_aggregation_exp(
    column_name: str,
    aggregation_method: AggregateMethod,
    processing_type: ProcessingType,
    metric_name: str,
    filter_expr: pl.Expr = None,
):
    """Build Aggregation expressions.

    Args:
        column_name (str): Column to run aggregations on
        aggregation_method (models.AggregateMethod): Aggregation method based on AggregateMethod
        processing_type (models.ProcessingType): Processing type depending on ProcessingType
        metric_name (str): Name of the metric
    """
    print("2 ", aggregation_method, type(aggregation_method))
    if processing_type == ProcessingType.IN_MEMORY:
        agg_exp = trend_repository.build_polar_aggregation_exp(
            column_name=column_name,
            aggregation_method=aggregation_method,
            filter_expr=filter_expr,
        ).alias(metric_name)
        size_expr = trend_repository.build_polar_aggregation_exp(
            column_name=column_name,
            aggregation_method=AggregateMethod.COUNT,
            filter_expr=filter_expr,
        ).alias("size")
    elif processing_type == ProcessingType.DATABRICKS:
        agg_exp = trend_repository.build_databricks_aggregation_exp(
            column_name=column_name, aggregation_method=aggregation_method
        )
        size_expr = trend_repository.build_databricks_aggregation_exp(
            column_name=column_name, aggregation_method=AggregateMethod.COUNT
        )  # rename col name to size
    else:
        agg_exp = None
    return [agg_exp, size_expr]


def build_combine_exp(
    metric: DualColumnMetric,
    processing_type: ProcessingType,
):
    """Build Aggregation expressions.

    Args:
        column_name (str): Column to run aggregations on
        aggregation_method (models.AggregateMethod): Aggregation method based on AggregateMethod
        processing_type (models.ProcessingType): Processing type depending on ProcessingType
    """
    print("2 ", metric.combine_method, type(metric.combine_method))
    if processing_type == ProcessingType.IN_MEMORY:
        numerator_metric_agg_expr = (
            trend_repository.build_polar_aggregation_exp(
                column_name=metric.numerator_metric.column,
                aggregation_method=metric.numerator_metric.aggregation_method,
                filter_expr=build_filter_exp(
                    metric.numerator_metric.filters, processing_type
                ),
            )
            .fill_null(0)
            .alias(metric.numerator_metric.name)
        )
        denominator_metric_agg_expr = (
            trend_repository.build_polar_aggregation_exp(
                column_name=metric.denominator_metric.column,
                aggregation_method=metric.denominator_metric.aggregation_method,
                filter_expr=build_filter_exp(
                    metric.denominator_metric.filters, processing_type
                ),
            )
            .fill_null(0)
            .alias(metric.denominator_metric.name)
        )
        dual_metric_agg_expr = trend_repository.build_polar_combine_exp(
            dual_metric_name=metric.name,
            numerator_metric_agg_expr=numerator_metric_agg_expr,
            denominator_metric_agg_expr=denominator_metric_agg_expr,
            combine_method=metric.combine_method,
        ).alias(metric.name)
        size_expr = trend_repository.build_polar_aggregation_exp(
            column_name=metric.numerator_metric.column,
            aggregation_method=AggregateMethod.COUNT,
            filter_expr=None,
        ).alias("size")
    elif processing_type == ProcessingType.DATABRICKS:
        numerator_metric_agg_expr = trend_repository.build_databricks_aggregation_exp(
            column_name=metric.numerator_metric.column, processing_type=processing_type
        )  # rename column to metric numerator name
        denominator_metric_agg_expr = trend_repository.build_databricks_aggregation_exp(
            column_name=metric.denominator_metric.column,
            processing_type=processing_type,
        )  # rename column to metric denominator name
        dual_metric_agg_expr = trend_repository.build_databricks_combine_exp(
            dual_metric_name=metric.name,
            numerator_metric_agg_expr=numerator_metric_agg_expr,
            denominator_metric_agg_expr=denominator_metric_agg_expr,
            combine_method=metric.combine_method,
        )
        size_expr = trend_repository.build_databricks_aggregation_exp(
            column_name=metric.numerator_metric.column,
            aggregation_method=AggregateMethod.COUNT,
        )  # rename col name to size
    else:
        numerator_metric_agg_expr = None
        denominator_metric_agg_expr = None
        dual_metric_agg_expr = None
        size_expr = None
    return [
        numerator_metric_agg_expr,
        denominator_metric_agg_expr,
        dual_metric_agg_expr,
        size_expr,
    ]


def build_filter_exp(filters: Filter, processing_type: ProcessingType):
    """Build filter expressions.

    Args:
        filters (models.Filter): Filters to apply on database
        processing_type (models.ProcessingType): Processing type depending on ProcessingType
    """
    if processing_type == ProcessingType.IN_MEMORY:
        filter_exp = trend_repository.build_polar_filter_exp(filters=filters)
    elif processing_type == ProcessingType.DATABRICKS:
        filter_exp = trend_repository.build_databricks_filter_exp(filters=filters)
    return filter_exp


def apply_filters(dataframe, filters_expr, processing_type: ProcessingType):
    """Apply filters to data.

    Args:
        dataframe: Dataframe on which to apply the filter
        filters_expr: Filters expression to run on data
        processing_type (models.ProcessingType): Processing type depending on ProcessingType
    """
    if processing_type == ProcessingType.IN_MEMORY:
        filtered_data = trend_repository.apply_polar_filters(
            dataframe=dataframe, filters_expr=filters_expr
        )
    elif processing_type == ProcessingType.DATABRICKS:
        filtered_data = trend_repository.apply_databricks_filters(
            dataframe=dataframe, filters_expr=filters_expr
        )
    return filtered_data


def apply_trend_agg(
    dataframe: Union[pl.DataFrame, Any],
    agg_expr: Union[pl.Expr, Any],
    processing_type: ProcessingType,
    time_intervals: int = 7,
):
    """Calculate KPI Trend for the given time intevervals and trend type.

    Args:
        date_column (str): _description_
        metrics_trend (models.MetricsTrendType): _description_
        dataframe (Union[pl.DataFrame, Any]): _description_
        agg_expr (Union[pl.Expr, Any]): _description_
        processing_type (models.ProcessingType): Processing type depending on ProcessingType
        time_intervals (int, optional): _description_. Defaults to 7.
    """
    if processing_type == ProcessingType.IN_MEMORY:
        trend_calc = trend_repository.apply_polar_trend_agg(
            dataframe=dataframe,
            agg_expr=agg_expr,
            time_intervals=time_intervals,
        )
    elif processing_type == ProcessingType.DATABRICKS:
        trend_calc = trend_repository.apply_databricks_trend_agg(date_column=None)

    return trend_calc


def get_baseline_and_comparison_df(
    dataframe: pl.DataFrame,
    insights: MetricsInsight,
    processing_type: ProcessingType = "in_memory",
):
    """Apply filter on subgroup impact dataframe to extract data for required dimension_value_pairs."""
    if processing_type == ProcessingType.IN_MEMORY:
        # get date column from metric
        if isinstance(insights.metrics, SingleColumnMetric):
            date_column = insights.metrics.date_column
        elif isinstance(insights.metrics, DualColumnMetric):
            date_column = insights.metrics.numerator_metric.date_column

        # for either of 'segment' or 'time period' both baseline and comparison should be present
        if (insights.baseline_segment and insights.comparison_segment) or (
            insights.baseline_time_period and insights.comparison_time_period
        ):
            # if baseline time period is present apply filter
            if insights.baseline_time_period:
                baseline_df = calculation_utils.apply_time_period_filter(
                    dataframe, date_column, insights.baseline_time_period
                )

            # if baseline segment is present apply filter
            if insights.baseline_segment:
                filters_expr = build_filter_exp(
                    insights.baseline_segment, processing_type
                )
                # if baseline_df is present pass baseline_df as input
                if insights.baseline_time_period:
                    baseline_df = apply_filters(
                        baseline_df, filters_expr, processing_type
                    )
                else:
                    baseline_df = apply_filters(
                        dataframe, filters_expr, processing_type
                    )

            # if comparison time period is present apply filter
            if insights.comparison_time_period:
                comparison_df = calculation_utils.apply_time_period_filter(
                    dataframe, date_column, insights.comparison_time_period
                )
            # else apply baselime time period filter if comparison not present
            elif insights.baseline_time_period:
                comparison_df = calculation_utils.apply_time_period_filter(
                    dataframe, date_column, insights.baseline_time_period
                )

            # if comparison segment is present apply filter
            if insights.comparison_segment:
                filters_expr = build_filter_exp(
                    insights.comparison_segment, processing_type
                )
                # if comparison_df is present pass comparison_df as input
                if insights.comparison_time_period or insights.baseline_time_period:
                    comparison_df = apply_filters(
                        comparison_df, filters_expr, processing_type
                    )
                else:
                    comparison_df = apply_filters(
                        dataframe, filters_expr, processing_type
                    )
            # apply baseline segment filter if both time period are present
            elif (
                insights.comparison_time_period
                and insights.baseline_time_period
                and insights.baseline_segment
            ):
                filters_expr = build_filter_exp(
                    insights.baseline_segment, processing_type
                )
                comparison_df = apply_filters(
                    comparison_df, filters_expr, processing_type
                )
        else:
            raise Exception("Segments Not Defined!")
        return baseline_df, comparison_df
    elif processing_type == ProcessingType.DATABRICKS:
        return True, True
