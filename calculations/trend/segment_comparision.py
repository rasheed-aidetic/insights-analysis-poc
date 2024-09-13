import calculations.trend.services as trend_services
import polars as pl
from base.general import ProcessingType
from base.insights import MetricsInsight
from base.metrics import DualColumnMetric, Metric, SingleColumnMetric
from base.metrics_trend import BaseMetricsTrend
from calculations import utils as calculation_utils
from calculations.trend.periodic_metrics_trend import PeriodicMetricsTrend
from polars import DataFrame


class SegmentComparison(BaseMetricsTrend):
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

    def calculate(self, dataframe: DataFrame):
        baseline_df, comparison_df = trend_services.get_baseline_and_comparison_df(
            dataframe=dataframe,
            insights=self.insights,
            processing_type=self.processing_type,
        )

        periodic_trend_calculator = PeriodicMetricsTrend(
            processing_type=self.processing_type
        )

        # calculate trend
        baseline_df, baseline_filtered_df = periodic_trend_calculator.calculate(
            baseline_df, self.insights.metrics
        )
        comparison_df, comparison_filtered_df = periodic_trend_calculator.calculate(
            comparison_df, self.insights.metrics
        )

        # update datelabel with segment name
        baseline_df = baseline_df.with_columns(
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

        comparison_df = comparison_df.with_columns(
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

        return (
            pl.concat([baseline_df, comparison_df]),
            baseline_filtered_df,
            comparison_filtered_df,
        )
