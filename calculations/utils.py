from datetime import date, datetime
from typing import List, Tuple

import polars as pl
from pandas import Timestamp

from base.general import Filter

PERIOD_WINDOW_SIZE = {"daily": 6, "weekly": 3, "monthly": 6, "quarterly": 3}


def get_time_period_date_label(
    date_column: str, dataframe: pl.DataFrame, period: str = None
):
    """Get Time Period Date Label"""
    if period is None:
        return dataframe.with_columns(DateLabel=pl.lit("Overall"))
    elif period == "weekly":
        return dataframe.with_columns(
            DateLabel=pl.col(date_column).apply(
                lambda date: "{:04d}WK{:02d}".format(
                    Timestamp(date).year, Timestamp(date).week
                )
            )
        )
    elif period == "monthly":
        return dataframe.with_columns(
            DateLabel=pl.col(date_column).apply(
                lambda date: "{:04d}M{:02d}({:s})".format(
                    Timestamp(date).year,
                    Timestamp(date).month,
                    Timestamp(date).month_name(),
                )
            )
        )
    elif period == "quarterly":
        return dataframe.with_columns(
            DateLabel=pl.col(date_column).apply(
                lambda date: "{:04d}Q{:d}".format(
                    Timestamp(date).year, Timestamp(date).quarter
                )
            )
        )
    elif period == "daily":  # daily YYYYMMDD
        return dataframe.with_columns(
            DateLabel=pl.col(date_column).apply(
                lambda date: "{:04d}{:02d}{:02d}".format(
                    Timestamp(date).year, Timestamp(date).month, Timestamp(date).day
                )
            )
        )


def get_date_from_date_label(
    df: pl.DataFrame, period: str, date_label_column="DateLabel"
):
    """
    Convert the `DateLabel` string back into date
    """

    def parse_quarter(date):
        year, quarter = date.upper().split("Q")
        dt = datetime(int(year), int(quarter) * 3 - 2, 1)
        return dt.strftime("%Y-%m-%d")

    if period == "weekly":
        df = df.with_columns(
            pl.col(date_label_column)
            .apply(lambda x: datetime.strptime(x + "-1", "%YWK%W-%w"))
            .cast(pl.Date)
            .alias("parsed_date")
        )

    elif period == "monthly":
        df = df.with_columns(
            pl.col(date_label_column)
            .apply(lambda x: datetime.strptime(x, "%Y%B"))
            .cast(pl.Date)
            .alias("parsed_date")
        )

    elif period == "quarterly":
        df = df.with_columns(
            pl.col(date_label_column)
            .apply(parse_quarter)
            .cast(pl.Date)
            .alias("parsed_date")
        )
    else:
        df = df.with_columns(
            pl.col(date_label_column).str.to_date(format="%Y%m%d").alias("parsed_date")
        )

    return df


def get_segment_col_name(
    base_str: str, filters: List[Filter], time_period: Tuple[date, date]
):
    segment_name = base_str
    if time_period:
        segment_name += f"__{time_period[0]}_{time_period[1]}"
    if filters:
        for filter in filters:
            segment_name += f"__{filter.column}_{filter.operator}_{filter.values}"

    return segment_name


def apply_time_period_filter(
    dataframe: pl.DataFrame, date_column: str, time_period: Tuple[date, date]
):
    return dataframe.filter(
        pl.col(date_column).ge(time_period[0]) & pl.col(date_column).le(time_period[1])
    )
