"""Utility functions realted to data loading from source"""

import os

import polars as pl


def load_df_from_csv(path: str, date_column: str = None):
    """Loading dataframe from CSV and typecasting the dates

    Args:
        path (str): path of the files.
        date_column (str, optional): Date column name in the dataframe. Defaults to None.

    Returns:
        pl.DataFrame: polars dataframe with proper date format
    """
    df = pl.read_csv(path, try_parse_dates=True)

    if date_column is not None and date_column in df.columns:
        d_type = df.dtypes[list(df.columns).index(date_column)]
    else:
        error_msg = "date_column not present in given CSV file!"
        print(error_msg)
        raise Exception(error_msg)

    if d_type == pl.Utf8:
        non_null_count = (
            df.filter(
                pl.col(date_column).str.lengths().gt(0)
                & pl.col(date_column).is_not_null()
            )
            .select(pl.col(date_column).count())
            .row(0)[0]
        )
        if non_null_count > 0:
            try:
                df = df.with_columns(
                    pl.col(date_column)
                    .str.to_date("%-m/%-d/%y %k:%M")
                    .alias(date_column)
                )
            except Exception as e:
                print("Exception as exc ", e)
    return df


def load_df_from_parquet(
    path: str, date_column: str = None, is_api_response: bool = False
):

    df = pl.read_parquet(path)
    print(df)

    if date_column is not None and date_column in df.columns:
        pass
    else:
        if not is_api_response:
            error_msg = "date_column not present in given Parquet file!"
            print(error_msg)
            raise Exception(error_msg)

    return df


def load_df_from_hdf5(path: str, date_column: str = None):
    pass


def load_df_from_databricks(
    **kwargs,
) -> (
    pl.DataFrame
):  # ./TODO: Replace kwargs with proper variables once the function is finalized
    """Loading dataframe from databricks and typecasting the dates

    Returns:
        pl.DataFrame: polars dataframe with proper date format
    """
    df: pl.DataFrame = None
    return df


def save_df_to_csv(df: pl.DataFrame, path: str, file_name: str):
    df.write_csv(os.path.join(path, file_name))


def save_df_to_parquet(df: pl.DataFrame, path: str, file_name: str):
    df.write_parquet(os.path.join(path, file_name))


def save_df_to_hdf5(df: pl.DataFrame, path: str, file_name: str):
    pass
