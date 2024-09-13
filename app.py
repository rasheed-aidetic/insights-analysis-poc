from flask import Flask, render_template, request, session, redirect, url_for, flash
import os
import pandas as pd

from time import time

import polars as pl

from base.general import Filter, FilterOperator
from base.insights import MetricsInsight
from base.metrics import DualColumnMetric, SingleColumnMetric
from calculations.anomaly.metric_anomaly import MetricAnomaly
from calculations.subgroup_insights.periodic_subgroup_insights_trend import (
    PeriodicSubgroupMetricsTrend,
)
from calculations.subgroup_insights.segment_subgroup_insights import (
    SegmentSubgroupInsights,
)
from calculations.trend.periodic_metrics_trend import PeriodicMetricsTrend
from calculations.trend.segment_comparision import SegmentComparison
from data_source import utils as data_source_utils

app = Flask(__name__)
app.secret_key = "your_secret_key"  # Replace with a secure random key


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        file_path = request.form["file_path"]
        date_column = request.form["date_column"]
        # Validate the file path
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                # Extract the columns
                columns = df.columns.tolist()
                # Store the columns in the session
                session["columns"] = columns
                session["date_column"] = date_column
                return redirect(url_for("define_metric"))
            except Exception as e:
                error_message = f"Error reading the file: {e}"
                return render_template("index.html", error=error_message)
        else:
            error_message = "Invalid file path or the file does not exist."
            return render_template("index.html", error=error_message)
    return render_template("index.html")


@app.route("/columns")
def show_columns():
    # Retrieve the columns from the session
    columns = session.get("columns", [])
    return render_template("columns.html", columns=columns)


@app.route("/define_metric", methods=["GET", "POST"])
def define_metric():
    columns = session.get("columns", [])
    if not columns:
        return redirect(url_for("index"))
    if request.method == "POST":
        try:
            metric_type = request.form.get("metric_type")
            metric_name = request.form.get("metric_name")
            filters = []

            # Process filters
            filter_columns = (
                request.form.getlist("single_filter_column[]")
                if metric_type == "single"
                else request.form.getlist("dual_filter_column[]")
            )
            filter_operators = (
                request.form.getlist("single_filter_operator[]")
                if metric_type == "single"
                else request.form.getlist("dual_filter_operator[]")
            )
            filter_values = (
                request.form.getlist("single_filter_value[]")
                if metric_type == "single"
                else request.form.getlist("dual_filter_value[]")
            )

            for col, op, val in zip(filter_columns, filter_operators, filter_values):
                if col and op and val:
                    filter_dict = {"column": col, "operator": op, "values": [val]}
                    filters.append(filter_dict)

            if metric_type == "single":
                # Extract single metric details
                column = request.form.get("column")
                date_column = request.form.get("date_column")
                aggregation_method = request.form.get("aggregation_method")

                metric = {
                    "name": metric_name,
                    "column": column,
                    "date_column": date_column,
                    "aggregation_method": aggregation_method,
                    "filters": filters,
                }
                session["metric"] = metric
                return redirect(url_for("define_insight"))
            elif metric_type == "dual":
                # Extract dual metric details
                # Numerator
                numerator_name = request.form.get("numerator_name")
                numerator_column = request.form.get("numerator_column")
                numerator_date_column = request.form.get("numerator_date_column")
                numerator_aggregation_method = request.form.get(
                    "numerator_aggregation_method"
                )

                # Denominator
                denominator_name = request.form.get("denominator_name")
                denominator_column = request.form.get("denominator_column")
                denominator_date_column = request.form.get("denominator_date_column")
                denominator_aggregation_method = request.form.get(
                    "denominator_aggregation_method"
                )

                combine_method = request.form.get("combine_method")

                numerator_metric = {
                    "name": numerator_name,
                    "column": numerator_column,
                    "date_column": numerator_date_column,
                    "aggregation_method": numerator_aggregation_method,
                    "filters": filters,  # You can have separate filters for numerator
                }
                denominator_metric = {
                    "name": denominator_name,
                    "column": denominator_column,
                    "date_column": denominator_date_column,
                    "aggregation_method": denominator_aggregation_method,
                    "filters": filters,  # You can have separate filters for denominator
                }

                metric = {
                    "name": metric_name,
                    "combine_method": combine_method,
                    "numerator_metric": numerator_metric,
                    "denominator_metric": denominator_metric,
                    "filters": filters,
                }
                session["metric"] = metric
                return redirect(url_for("define_insight"))
            else:
                flash("Invalid metric type selected.")
                return redirect(url_for("define_metric"))
        except Exception as e:
            flash(f"An error occurred: {e}")
            return redirect(url_for("define_metric"))
    else:
        return render_template("define_metric.html", columns=columns)


def initialize_metric(metric_data):
    # Initialize filters for the metric
    filters = []
    for f in metric_data.get("filters", []):
        operator = getattr(FilterOperator, f["operator"], None)
        if operator is None:
            continue
        filter_obj = Filter(column=f["column"], operator=operator, values=f["values"])
        filters.append(filter_obj)

    if "column" in metric_data:  # Single column metric
        metric = SingleColumnMetric(
            name=metric_data["name"],
            column=metric_data["column"],
            date_column=metric_data["date_column"],
            aggregation_method=metric_data["aggregation_method"],
            filters=filters,
        )
    else:  # Dual column metric
        numerator_metric_data = metric_data.get("numerator_metric")
        denominator_metric_data = metric_data.get("denominator_metric")

        # Initialize numerator metric
        numerator_filters = []
        for f in numerator_metric_data.get("filters", []):
            operator = getattr(FilterOperator, f["operator"], None)
            if operator is None:
                continue
            filter_obj = Filter(
                column=f["column"], operator=operator, values=f["values"]
            )
            numerator_filters.append(filter_obj)

        numerator_metric = SingleColumnMetric(
            name=numerator_metric_data["name"],
            column=numerator_metric_data["column"],
            date_column=numerator_metric_data["date_column"],
            aggregation_method=numerator_metric_data["aggregation_method"],
            filters=numerator_filters,
        )

        # Initialize denominator metric
        denominator_filters = []
        for f in denominator_metric_data.get("filters", []):
            operator = getattr(FilterOperator, f["operator"], None)
            if operator is None:
                continue
            filter_obj = Filter(
                column=f["column"], operator=operator, values=f["values"]
            )
            denominator_filters.append(filter_obj)

        denominator_metric = SingleColumnMetric(
            name=denominator_metric_data["name"],
            column=denominator_metric_data["column"],
            date_column=denominator_metric_data["date_column"],
            aggregation_method=denominator_metric_data["aggregation_method"],
            filters=denominator_filters,
        )

        metric = DualColumnMetric(
            name=metric_data["name"],
            combine_method=int(metric_data["combine_method"]),
            numerator_metric=numerator_metric,
            denominator_metric=denominator_metric,
            filters=filters,
        )

    return metric

def get_insight_results(insights):

    file_path = session["file_path"]
    date_column = session["date_column"]
    df = data_source_utils.load_df_from_csv(
    file_path,
    date_column=date_column,
)
    # calculate overall trends based on period
    trend_calculator = SegmentComparison(insights=insights)
    trend_df = trend_calculator.calculate(
        df,
    )
    print("--------------------------------")
    trend_df[1].head()
    # calculate trends for subgroups based on period
    subgroup_trend_calculator = SegmentSubgroupInsights(insights=insights)
    subgroup_trend_df = subgroup_trend_calculator.calculate(
        trend_df=trend_df[0],
        baseline_filtered_df = trend_df[1],
        comparison_filtered_df = trend_df[2],
    )
    subgroup_trend_df.head()
    subgroup_trend_df.write_csv("test_data_2.csv")
    return subgroup_trend_df.to_dicts()


@app.route("/define_insight", methods=["GET", "POST"])
def define_insight():
    columns = session.get("columns", [])
    metric_data = session.get("metric", None)
    if not columns or not metric_data:
        return redirect(url_for("index"))
    if request.method == "POST":
        insight_name = request.form.get("insight_name")
        group_by_columns = request.form.getlist("group_by_columns[]")

        # Process baseline filters
        baseline_filters = []
        baseline_columns = request.form.getlist("baseline_filter_column[]")
        baseline_operators = request.form.getlist("baseline_filter_operator[]")
        baseline_values = request.form.getlist("baseline_filter_value[]")

        for col, op, val in zip(baseline_columns, baseline_operators, baseline_values):
            if col and op and val:
                filter_dict = {"column": col, "operator": op, "values": [val]}
                baseline_filters.append(filter_dict)

        # Process comparison filters
        comparison_filters = []
        comparison_columns = request.form.getlist("comparison_filter_column[]")
        comparison_operators = request.form.getlist("comparison_filter_operator[]")
        comparison_values = request.form.getlist("comparison_filter_value[]")

        for col, op, val in zip(
            comparison_columns, comparison_operators, comparison_values
        ):
            if col and op and val:
                filter_dict = {"column": col, "operator": op, "values": [val]}
                comparison_filters.append(filter_dict)

        # Initialize filters for baseline and comparison segments
        baseline_segment = []
        for f in baseline_filters:
            operator = getattr(FilterOperator, f["operator"], None)
            if operator is None:
                continue
            filter_obj = Filter(
                column=f["column"], operator=operator, values=f["values"]
            )
            baseline_segment.append(filter_obj)

        comparison_segment = []
        for f in comparison_filters:
            operator = getattr(FilterOperator, f["operator"], None)
            if operator is None:
                continue
            filter_obj = Filter(
                column=f["column"], operator=operator, values=f["values"]
            )
            comparison_segment.append(filter_obj)

        # Initialize the metric object
        metric = initialize_metric(metric_data)

        # Initialize the MetricsInsight object
        insights = MetricsInsight(
            name=insight_name,
            metrics=metric,
            group_by_columns=group_by_columns,
            baseline_segment=baseline_segment,
            comparison_segment=comparison_segment,
        )
        insight_results = get_insight_results(insights)

        # Store insights in the session or process it further
        session["insights"] = insight_results

        # Proceed with analysis or further processing
        return render_template("insight_result.html", insights=insight_results)
    else:
        return render_template(
            "define_insight.html", columns=columns, metric=metric_data
        )


@app.route("/insight_result")
def insight_result():
    insights = session.get("insights", None)
    if not insights:
        return redirect(url_for("index"))
    return render_template("insight_result.html", insights=insights)


if __name__ == "__main__":
    app.run(debug=True)
