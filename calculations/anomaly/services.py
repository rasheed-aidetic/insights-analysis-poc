import polars as pl

from base.general import ProcessingType
from base.metrics_trend import MetricsTrendType
from calculations.anomaly import repository as anomaly_repository


def calculate_anomaly(
    trend_df: pl.DataFrame,
    metric_name,
    metric_trend_type: MetricsTrendType,
    threshold: float,
    processing_type: str,
    sub_group: bool = False,
):
    if processing_type == ProcessingType.IN_MEMORY:
        results, z_score_col = anomaly_repository.calculate_polars_anomaly(
            trend_df, metric_name, metric_trend_type, sub_group
        )
        results = anomaly_repository.tag_polars_anomalies(
            z_score_col, results, threshold
        )

    elif processing_type == ProcessingType.DATABRICKS:
        results, z_score_col = anomaly_repository.calculate_databricks_anomaly()
        results = anomaly_repository.tag_databricks_anomalies()

    return results


def calculate_row_diff_anomaly(
    trend_df: pl.DataFrame,
    metric_name,
    metric_trend_type: str,
    threshold: float,
    processing_type: str,
    sub_group: bool = False,
):
    if processing_type == ProcessingType.IN_MEMORY:
        results, z_score_col = anomaly_repository.calculate_polars_rowdiff_anomaly(
            trend_df, metric_name, metric_trend_type, sub_group
        )
        results = anomaly_repository.tag_polars_anomalies(
            z_score_col, results, threshold, "is_anomalous_diff"
        )

    elif processing_type == ProcessingType.DATABRICKS:
        results, z_score_col = anomaly_repository.calculate_databricks_rowdiff_anomaly()
        results = anomaly_repository.tag_databricks_anomalies()

    return results


def calculate_yoy_diff_anomaly(
    trend_df: pl.DataFrame,
    metric_name,
    metric_trend_type: str,
    threshold: float,
    processing_type: str,
    sub_group: bool = False,
):
    if processing_type == ProcessingType.IN_MEMORY:
        results, z_score_col = anomaly_repository.calculate_polars_yoy_diff_anomaly(
            trend_df, metric_name, metric_trend_type, sub_group
        )
        results = anomaly_repository.tag_polars_anomalies(
            z_score_col, results, threshold, "is_anomalous_YoY_diff"
        )

    elif processing_type == ProcessingType.DATABRICKS:
        (
            results,
            z_score_col,
        ) = anomaly_repository.calculate_databricks_yoy_diff_anomaly()
        results = anomaly_repository.tag_databricks_anomalies()

    return results
