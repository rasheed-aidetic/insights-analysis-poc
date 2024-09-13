from typing import List

import polars as pl
from polars import Expr

from base.general import AggregateMethod, CombineMethod, Filter, FilterOperator
from base.metrics_trend import MetricsTrendType
from calculations import utils as calculation_utils


def build_polar_aggregation_exp(
    column_name: str,
    aggregation_method: AggregateMethod,
    filter_expr: pl.Expr,
) -> Expr:
    """Build polar aggregation expressions for in-memory calculation

    Args:
        column_name (str): Column on which to run aggregations
        aggregation_method (AggregateMethod): Type of aggregation on single column.
            Look at the AggregateMethod
    """
    col = pl.col(column_name)
    if filter_expr is not None:
        col = col.filter(filter_expr)
    print(
        aggregation_method,
    )
    if aggregation_method == AggregateMethod.SUM:
        return col.sum()
    elif aggregation_method == AggregateMethod.COUNT:
        return col.count()
    elif aggregation_method == AggregateMethod.DISTINCT:
        return col.n_unique().cast(int)
    elif aggregation_method == AggregateMethod.AVG:
        return col.mean()
    else:
        accepted_values = [member.value for member in AggregateMethod]
        raise ValueError(f"aggregation_method should be either of {accepted_values}")


def build_databricks_aggregation_exp(**kwargs):
    """Build aggregate expressions for databricks"""
    kwargs = None
    return kwargs


def build_polar_combine_exp(
    dual_metric_name: str,
    combine_method: CombineMethod,
    numerator_metric_agg_expr: Expr,
    denominator_metric_agg_expr: Expr,
) -> Expr:
    """Build polar aggregation expressions for in-memory calculation

    Args:
        column_name (str): Column on which to run aggregations
        combine_method (CombineMethod): Type of combination to apply on dual column metric.
            Look at the CombineMethod class in models
    """
    if combine_method == CombineMethod.RATIO:
        dual_metric_agg_expr = (
            pl.when(
                (denominator_metric_agg_expr == 0)
                | numerator_metric_agg_expr.is_null()
                | denominator_metric_agg_expr.is_null()
            )
            .then(0)
            .otherwise(numerator_metric_agg_expr / denominator_metric_agg_expr)
            .alias(dual_metric_name)
        )
    else:
        accepted_values = [member.value for member in CombineMethod]
        raise ValueError(f"combine_method should be either of {accepted_values}")
    return dual_metric_agg_expr


def build_databricks_combine_exp(**kwargs):
    """Build aggregate expressions for databricks"""
    kwargs = None
    return kwargs


def apply_polar_trend_agg(
    dataframe: pl.DataFrame,
    agg_expr: pl.Expr,
    time_intervals: int = None,
):
    """Calculate trend of KPI for polars dataset

    Args:
        dataframe (pl.DataFrame): dataframe to run calculations on
        agg_expr (pl.Expr): aggregation expression
        time_period (int, optional): time intervals for which to aggregate the KPI for. Defaults to 7 intervals.

    Returns:
        _type_: _description_
    """

    trend_df = (
        dataframe.group_by("DateLabel").agg(agg_expr).sort("DateLabel", descending=True)
    )

    if time_intervals is not None:
        trend_df = trend_df[-1 * (time_intervals + 1) :]

    return trend_df  # .to_dicts()


def apply_databricks_trend_agg(**kwargs):
    """Calculate trend of KPI for databricks data"""
    kwargs = None
    return kwargs


def build_polar_filter_exp(filters: List[Filter]):
    """Build filter expression for in-memory data processing

    Args:
        filters (Filter): Filters to apply on database
    """
    filter_expr = pl.lit(True)

    if filters is not None:
        for _filter in filters:
            expr = pl.col(_filter.column)  # .cast(pl.Utf8)
            if _filter.operator == FilterOperator.EQ:
                expr = expr.is_in(_filter.values)
            elif _filter.operator == FilterOperator.NEQ:
                expr = expr.is_in(_filter.values).is_not()
            elif _filter.operator == FilterOperator.EMPTY:
                expr = expr.is_null() | expr.len().eq(0)
            elif _filter.operator == FilterOperator.NON_EMPTY:
                expr = expr.is_not_null() & expr.len().gt(0)
            elif _filter.operator == FilterOperator.GRT:
                expr = expr.gt(_filter.values[0])
            elif _filter.operator == FilterOperator.GEQ:
                expr = expr.ge(_filter.values[0])
            elif _filter.operator == FilterOperator.LWT:
                expr = expr.lt(_filter.values[0])
            elif _filter.operator == FilterOperator.LEQ:
                expr = expr.le(_filter.values[0])

            filter_expr = filter_expr & expr

    return filter_expr


def build_databricks_filter_exp(**kwargs):
    """Build filter expressions for databricks"""
    kwargs = None
    return kwargs


def apply_polar_filters(dataframe: pl.DataFrame, filters_expr: pl.Expr):
    """Apply filters to in-memory data

    Args:
        dataframe (pl.DataFrame): Polars dataframe for in-memory processing
        filters_expr (pl.Expr): Filter expression to apply on data
    """
    return dataframe.filter(filters_expr)


def apply_databricks_filters(**kwargs):
    """Apply filters to databricks data"""
    kwargs = None
    return kwargs
