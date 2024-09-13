from abc import ABC
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel

from base.general import Filter
from base.metrics import DualColumnMetric, Metric, SingleColumnMetric

# @dataclass
# class Dimension:
#     """Dimension dataclass for all dimensions"""

#     name: str
#     score: float
#     is_key_dimension: bool


class DimensionValuePair(BaseModel):
    """Dataclass for a dimension and valur pair"""

    dimension: str = None
    value: Any = None


class DataCutInfo(BaseModel):
    """Dataclass for data cut information in metrics insight output"""

    dimension_value: Tuple[DimensionValuePair]
    baseline_value: float
    baseline_number_of_rows: int
    impact_float: float
    comparison_value: float = None
    comparison_number_of_row: int = None


# Metric Insights can be


class MetricsInsight(BaseModel):
    """Dataclass for metrics insight output"""

    # Inputs of Metric Insight
    name: str
    metrics: SingleColumnMetric | DualColumnMetric
    group_by_columns: List[str]
    max_rca_depth: int = 3
    baseline_time_period: Tuple[date, date] = None
    baseline_segment: List[Filter] = None
    comparison_time_period: Tuple[date, date] = None
    comparison_segment: List[Filter] = None
    # baseline related outputs
    total_segments: int = None
    baseline_number_of_rows: int = None
    baseline_value: float = None
    # comparision related outputss
    comparison_number_of_row: int = None
    comparison_value: float = None
    # List of all data cuts
    # data_cut_info: List[Dict[str, DataCutInfo]] = None


def create_metric_insight_from_metric_details(
    metric_details: Metric, group_by_columns: List[str]
):
    """Create MetricInsight from metric details and group by columns.

    Args:
        metric_details (Metric): metric details for any metric it must be of type
            SingleColoumnMetric or DualColumnMetric.
        group_by_columns (List[str]): dimensions on which to run the analysis.

    Raises:
        TypeError: Raise type error in case the metric_details type is
            not of type either SingleColumnMetric or DualColumnMetric.

    Returns:
        insight_details: MetricInsight object containing details of metrics and group_by_column.
        date_column: name of columns containing date in the datamodel.
    """
    if isinstance(metric_details, DualColumnMetric):
        print("Process for dual metric")
        date_column = metric_details.numerator_metric.date_column
    elif isinstance(metric_details, SingleColumnMetric):
        print("Process for single metric")
        date_column = metric_details.date_column
    else:
        raise TypeError(
            "The metric should be either of category SingleColumnMetric or DualColumnMetric."
        )

    insight_details = MetricsInsight(
        **{
            "name": metric_details.name,
            "metrics": metric_details,
            "group_by_columns": group_by_columns,
        }
    )
    return insight_details, date_column
