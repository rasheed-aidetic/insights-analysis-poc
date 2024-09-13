from abc import ABC, abstractmethod
from typing import Dict, Iterable, List

from polars import Expr
from pydantic import BaseModel

from base.general import AggregateMethod, CombineMethod, Filter, ProcessingType


class Metric(ABC, BaseModel):
    """Base class for different type of Metric"""

    name: str  # kpi name
    filters: List[Filter] = None  # list of filter Filter(col, op, value)

    # @abstractmethod
    # def get_name(self):
    #     """Get name of the metrics"""

    # @abstractmethod
    # def get_aggregation_exprs(self):
    #     """Get the Expression of the metrics to run on database"""

    # @abstractmethod
    # def get_metric_type(self):
    #     """Aggregation type of the metric"""

    # @abstractmethod
    # def get_sorting_expr(self):
    #     """Get sorting expr"""


class SingleColumnMetric(Metric):
    """Single column metric, Sum, Count, Distinct, Avg"""

    column: str  # column name in db
    date_column: str  # column name for date
    aggregation_method: AggregateMethod  # sum / avg /etc
    dimensions: List[str] = None  # group by columns

    # def get_name(self):
    #     return self.name

    # def get_aggregation_exprs(
    #     self, processing_type: ProcessingType = "in_memory"
    # ) -> Iterable[Expr]:
    #     pass

    # def get_metric_type(self):
    #     return self.aggregation_method.name

    # def get_sorting_expr(self):
    #     pass


class DualColumnMetric(Metric):
    """Dual Column Metric"""

    combine_method: CombineMethod
    numerator_metric: SingleColumnMetric
    denominator_metric: SingleColumnMetric
    dimensions: List[str] = None  # group by columns

    # def get_name(self):
    #     return self.name

    # def get_aggregation_exprs(
    #     self, processing_type: ProcessingType = "in_memory"
    # ) -> Iterable[Expr]:
    #     pass

    # def get_metric_type(self):
    #     return self.combine_method.name

    # def get_sorting_expr(self):
    #     pass


def parse_metrics_from_dict(metrics_details: Dict):
    """Parse a dictionary to SingColumnMetric or DualColumn Metric, depending on
    the presence of aggregation_method and numerator_metric respectively.

    Args:
        metrics_details (Dict): A dictionary containing all the details regarding a metric.
    """
    if "filters" in metrics_details:  # Parse primary filter
        metrics_details["filters"] = parse_filters_from_dict(
            metrics_details.get("filters")
        )

    if "numerator_metric" in metrics_details:
        print("Parsing DualColumnMetric")

        # Parse filters in numerator or denominator metric
        dependent_metrics = ["numerator_metric", "denominator_metric"]
        for _dependent_metric in dependent_metrics:
            if "filters" in metrics_details.get(_dependent_metric):
                metrics_details[_dependent_metric]["filters"] = parse_filters_from_dict(
                    metrics_details.get(_dependent_metric).get("filters")
                )

            # COMMENT: if filter is not present, should still run this
            metrics_details[_dependent_metric] = SingleColumnMetric(
                **metrics_details.get(_dependent_metric)
            )

        # Parse the DualColumnMetric
        parsed_metrics_detail = DualColumnMetric(**metrics_details)
    elif "aggregation_method" in metrics_details:

        print("Parsing SingColumnMetric")
        parsed_metrics_detail = SingleColumnMetric(**metrics_details)
    else:
        raise TypeError(
            "The metric should be either of category SingleColumnMetric or DualColumnMetric."
        )

    return parsed_metrics_detail


def parse_filters_from_dict(filter_list: List[Dict]):
    """Parse a dictionary containing filter details to Filter data type.

    Args:
        filter_list (List[Dict]): List of dictionaries containing details of filters.
    """
    parsed_filters = []
    for _filter in filter_list:
        parsed_filters.append(Filter(**_filter))
    return parsed_filters
