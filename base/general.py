from datetime import date
from enum import Enum
from typing import List, Optional, Union

from aenum import StrEnum
from pydantic import BaseModel


class FilterOperator(StrEnum):
    """Filter operators to filter dataframe"""

    EQ = "equal_to"
    NEQ = "not_equal_to"
    EMPTY = "empty"
    NON_EMPTY = "non_empty"
    GRT = "greater_than"
    LWT = "lower_than"
    GEQ = "greater_than_equal_to"
    LEQ = "lower_than_equal_to"


class Filter(BaseModel):
    """Filter to apply on a coloumn."""

    column: str
    operator: FilterOperator
    values: Optional[List[Union[str, float, bool, date]]] = None


class AggregateMethod(StrEnum):
    """Method to aggregation for a single column to calculate the KPI"""

    COUNT = "count"
    DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "average"


class CombineMethod(Enum):
    """Method to combine multiple columns to calculate KPI"""

    RATIO = 1


class ProcessingType(StrEnum):
    """Processing Location"""

    IN_MEMORY = "in_memory"
    DATABRICKS = "databricks"
