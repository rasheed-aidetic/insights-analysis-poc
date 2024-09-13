from base.general import ProcessingType
from base.insights import MetricsInsight
from base.metrics import DualColumnMetric, SingleColumnMetric
from base.metrics_trend import BaseMetricsTrend
from calculations.subgroup_insights import services as subgroup_trend_services
from calculations.trend import services as trend_services


class PeriodicSubgroupMetricsTrend(BaseMetricsTrend):
    """Periodic Subgroup Metrics Trend"""

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
        trend_df,
        dataframe,
        metric_details: MetricsInsight,
    ):
        """
        Calculate Periodic Subgroup Metrics Trend

        Args:

            dataframe (_type_): dataframe on which to run calculation
            trend_df: Periodic trends for metric
            metric (models.Metric): Metrics type to calculate

            we may do customization based on metric type in future,or how we return/format the response based on metric type
        """

        columns_to_combine = self.get_columns_to_combine(metric_details=metric_details)

        all_combinations = subgroup_trend_services.build_dimension_combinations(
            columns_to_combine, rca_depth=metric_details.max_rca_depth
        )
        if isinstance(metric_details.metrics, SingleColumnMetric):
            print(
                "Aggregation Method ",
                metric_details.metrics.aggregation_method,
                type(metric_details.metrics.aggregation_method),
            )

            agg_expr = trend_services.build_aggregation_exp(
                column_name=metric_details.metrics.column,
                aggregation_method=metric_details.metrics.aggregation_method,
                processing_type=self.processing_type,
                metric_name=metric_details.name,
            )

            trend_calc = subgroup_trend_services.apply_subgroup_trend_agg(
                all_combinations=all_combinations,
                trend_df=trend_df,
                dataframe=dataframe,
                agg_expr=agg_expr,
                processing_type=self.processing_type,
                time_intervals=self.time_intervals,
                metric_details=metric_details,
            )
        elif isinstance(metric_details.metrics, DualColumnMetric):
            pass
            print(
                "Aggregation Method ",
                metric_details.metrics.combine_method,
                type(metric_details.metrics.combine_method),
            )

            agg_expr = trend_services.build_combine_exp(
                metric=metric_details.metrics,
                processing_type=self.processing_type,
            )
            # passing date column of numerator metric here
            trend_calc = subgroup_trend_services.apply_subgroup_trend_agg(
                all_combinations=all_combinations,
                trend_df=trend_df,
                dataframe=dataframe,
                agg_expr=agg_expr,
                processing_type=self.processing_type,
                time_intervals=self.time_intervals,
                metric_details=metric_details,
            )

        return trend_calc
