from base.general import ProcessingType
from base.metrics import DualColumnMetric, Metric, SingleColumnMetric
from base.metrics_trend import BaseMetricsTrend, MetricsTrendType
from calculations import utils as calculation_utils
from calculations.trend import services as trend_services


class PeriodicMetricsTrend(BaseMetricsTrend):
    """Periodic Metrics Trend"""

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
        dataframe,
        metric: Metric,
        metrics_trend: MetricsTrendType = None,
    ):
        """Calculate Periodic Metrics Trend

        Args:
            dataframe (_type_): dataframe on which to run calculation
            metric (models.Metric): Metrics type to calculate
            metrics_trend (models.MetricsTrendType): Metrics trend type for the time period to roll up the calculations

            we may do customization based on metric type in future,or how we return/format the response based on metric type
        """

        # apply metric filter on DF
        filters_expr = trend_services.build_filter_exp(
            metric.filters, self.processing_type
        )
        dataframe = trend_services.apply_filters(
            dataframe, filters_expr, self.processing_type
        )
        print("filter")
        print(dataframe.head())

        if isinstance(metric, SingleColumnMetric):
            print(
                "Aggregation Method ",
                metric.aggregation_method,
                type(metric.aggregation_method),
            )
            dataframe = calculation_utils.get_time_period_date_label(
                date_column=metric.date_column,
                dataframe=dataframe,
                period=metrics_trend,
            )
            print("get_time_period_date_label")
            print("metrics_trend: ", metrics_trend)
            print(type(metrics_trend))
            print(dataframe.head())
            agg_expr = trend_services.build_aggregation_exp(
                column_name=metric.column,
                aggregation_method=metric.aggregation_method,
                processing_type=self.processing_type,
                metric_name=metric.name,
            )
            trend_calc = trend_services.apply_trend_agg(
                dataframe=dataframe,
                agg_expr=agg_expr,
                processing_type=self.processing_type,
                time_intervals=self.time_intervals,
            )
        elif isinstance(metric, DualColumnMetric):
            print(
                "Aggregation Method ",
                metric.combine_method,
                type(metric.combine_method),
            )
            dataframe = calculation_utils.get_time_period_date_label(
                date_column=metric.numerator_metric.date_column,
                dataframe=dataframe,
                period=metrics_trend,
            )
            agg_expr = trend_services.build_combine_exp(
                metric=metric,
                processing_type=self.processing_type,
            )
            # passing date column of numerator metric here
            trend_calc = trend_services.apply_trend_agg(
                dataframe=dataframe,
                agg_expr=agg_expr,
                processing_type=self.processing_type,
                time_intervals=self.time_intervals,
            )

        return trend_calc, dataframe
