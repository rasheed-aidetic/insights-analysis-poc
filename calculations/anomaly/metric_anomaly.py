"""Metric analmoly calculations"""

import polars as pl

from base.anomaly import Anomaly
from base.general import ProcessingType
from base.insights import MetricsInsight
from base.metrics import Metric
from base.metrics_trend import MetricsTrendType
from calculations.anomaly import services as anomaly_services


class MetricAnomaly(Anomaly):
    def __init__(
        self,
        processing_type: ProcessingType = "in_memory",
        time_intervals: int = None,
    ) -> None:
        super().__init__()
        self.processing_type = processing_type
        self.time_intervals = time_intervals

    def calculate(
        self,
        df: pl.DataFrame,
        trend_df: pl.DataFrame,
        metric_trend_type: str,
        metric_details: MetricsInsight,
        threshold=1.0,
    ) -> dict[str, pl.DataFrame]:
        """
        Calculate the anomalies based on z-score

        Args:
            df (polars.DataFrame): The original data
            trend_df (polars.DataFrame): The trend df calculated using the metric and the period
            metric (SingleColumnMetric|DualColumnMetric): The metric to find anomalies on
            threshold (float): The z-score threshold over which are considered anomalies
        """

        trend_df = trend_df.sort(["DateLabel"], descending=False)

        trend_df = anomaly_services.calculate_anomaly(
            trend_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
        )

        trend_df = anomaly_services.calculate_row_diff_anomaly(
            trend_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
        )

        trend_df = anomaly_services.calculate_yoy_diff_anomaly(
            trend_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
        )

        return trend_df

    def calculate_subgroups(
        self,
        subgroup_df: pl.DataFrame,
        metric_trend_type: MetricsTrendType,
        metric_details: MetricsInsight,
        threshold=1.0,
    ):
        """
        Calculate anomalies based on z-score for subgroups
        """

        # sort based on subgroup and datelabel
        subgroup_df = subgroup_df.sort(
            ["sub_group", "DateLabel"], descending=[False, False]
        )

        subgroup_df = anomaly_services.calculate_anomaly(
            subgroup_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
            True,
        )

        subgroup_df = anomaly_services.calculate_row_diff_anomaly(
            subgroup_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
            True,
        )

        subgroup_df = anomaly_services.calculate_yoy_diff_anomaly(
            subgroup_df,
            metric_details.name,
            metric_trend_type,
            threshold,
            self.processing_type,
            True,
        )

        return subgroup_df
