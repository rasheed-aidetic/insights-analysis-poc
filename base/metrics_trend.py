from abc import abstractmethod

from aenum import StrEnum
from polars import DataFrame
from base.insights import MetricsInsight
from base.metrics import Metric


class MetricsTrendType(StrEnum):
    """Type of Metrics Trend"""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class BaseMetricsTrend:
    """Base class to calculate Metrics Trend"""

    metrics: Metric
    period: MetricsTrendType

    @abstractmethod
    def calculate(
        self, dataframe: DataFrame, metric: Metric, metrics_trend: MetricsTrendType
    ):
        """Calculate Trend based on requirement

        Args:
            dataframe (DataFrame): polar dataframe containing the trends
        """

    def get_columns_to_combine(self, metric_details: MetricsInsight):
        """Get all columns to combine for the calculation

        Args:
            metric_details (MetricsInsight): Metrics details
        """
        columns_to_combine = []
        if metric_details.group_by_columns:
            columns_to_combine.extend(metric_details.group_by_columns)
        if metric_details.metrics.dimensions:
            columns_to_combine.extend(metric_details.metrics.dimensions)
        return list(set(columns_to_combine))
