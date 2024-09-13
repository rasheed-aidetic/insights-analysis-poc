import calculations.trend.services as trend_services
import polars as pl
from base.general import ProcessingType
from base.insights import MetricsInsight
from base.metrics import DualColumnMetric, SingleColumnMetric
from base.metrics_trend import BaseMetricsTrend
from calculations import utils as calculation_utils
from calculations.subgroup_insights import services as subgroup_trend_services
from calculations.subgroup_insights.periodic_subgroup_insights_trend import (
    PeriodicSubgroupMetricsTrend,
)
from polars import DataFrame


class SegmentSubgroupInsights(BaseMetricsTrend):
    def __init__(
        self,
        insights: MetricsInsight,
        processing_type: ProcessingType = "in_memory",
        time_intervals: int = None,
    ) -> None:
        super().__init__()
        self.processing_type = processing_type
        self.time_intervals = time_intervals
        self.insights = insights

    def calculate(
        self,
        trend_df: DataFrame,
        baseline_filtered_df: pl.DataFrame,
        comparison_filtered_df: pl.DataFrame,
    ):
        # update datelabel with segment name
        # need this step in subgroup first
        baseline_filtered_df = baseline_filtered_df.with_columns(
            DateLabel=pl.lit(
                calculation_utils.get_segment_col_name(
                    "baseline",
                    self.insights.baseline_segment,
                    self.insights.baseline_time_period,
                )
            )
        )

        comparison_segment = self.insights.comparison_segment
        if self.insights.comparison_segment:
            pass
        elif (
            self.insights.comparison_time_period
            and self.insights.baseline_time_period
            and self.insights.baseline_segment
        ):
            comparison_segment = self.insights.baseline_segment

        comparison_filtered_df = comparison_filtered_df.with_columns(
            DateLabel=pl.lit(
                calculation_utils.get_segment_col_name(
                    "comparison",
                    comparison_segment,
                    (
                        self.insights.comparison_time_period
                        if self.insights.comparison_time_period
                        else self.insights.baseline_time_period
                    ),
                )
            )
        )

        periodic_subgroup_trend_calculator = PeriodicSubgroupMetricsTrend(
            processing_type=self.processing_type
        )

        # calculate trend
        baseline_df = periodic_subgroup_trend_calculator.calculate(
            trend_df=trend_df,
            dataframe=baseline_filtered_df,
            metric_details=self.insights,
        )
        comparison_df = periodic_subgroup_trend_calculator.calculate(
            trend_df=trend_df,
            dataframe=comparison_filtered_df,
            metric_details=self.insights,
        )

        baseline_df = (
            subgroup_trend_services.merge_segment_baseline_and_comparison_data(
                baseline_df=baseline_df,
                comparison_df=comparison_df,
                metric_name=self.insights.name,
            )
        )

        return baseline_df
