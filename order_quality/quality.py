"""Reusable quality checks, quarantine shaping, and metrics generation."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from order_quality.silver import EXPECTED_ORDER_COLUMNS, REQUIRED_COLUMNS


VALID_STATUSES = ["CREATED", "PAID", "SHIPPED", "CANCELLED", "RETURNED"]


def _reason_if(condition: F.Column, reason_code: str) -> F.Column:
    return F.when(condition, F.array(F.lit(reason_code))).otherwise(F.array())


def schema_drift_checks(df: DataFrame, expected_schema: Mapping[str, str]) -> DataFrame:
    """Return one row per schema contract check."""
    spark = df.sparkSession
    actual_columns = set(df.columns)
    expected_columns = set(expected_schema)
    actual_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    rows = []
    for column_name in sorted(expected_columns - actual_columns):
        rows.append(("schema_missing_column", "FAIL", column_name, None, expected_schema[column_name]))
    for column_name in sorted(actual_columns - expected_columns):
        if not column_name.startswith("_") and column_name != "is_duplicate_order_id":
            rows.append(("schema_extra_column", "WARN", column_name, actual_types[column_name], None))
    for column_name in sorted(expected_columns & actual_columns):
        actual_type = actual_types[column_name]
        expected_type = expected_schema[column_name]
        status = "PASS" if actual_type == expected_type else "FAIL"
        rows.append(("schema_type_match", status, column_name, actual_type, expected_type))

    return spark.createDataFrame(
        rows,
        "check_type string, status string, column_name string, actual_type string, expected_type string",
    ).withColumn("checked_at", F.current_timestamp())


def apply_order_quality_checks(silver_orders: DataFrame) -> DataFrame:
    """Attach row-level reason codes before records are allowed into Gold."""
    reasons = F.concat(
        _reason_if(F.col("order_id").isNull() | (F.trim("order_id") == ""), "NULL_ORDER_ID"),
        _reason_if(F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        _reason_if(F.col("order_ts").isNull(), "NULL_OR_INVALID_ORDER_TS"),
        _reason_if(F.col("region").isNull(), "NULL_REGION"),
        _reason_if(F.col("product_id").isNull(), "NULL_PRODUCT_ID"),
        _reason_if(F.col("quantity").isNull(), "NULL_QUANTITY"),
        _reason_if(F.col("unit_price").isNull(), "NULL_UNIT_PRICE"),
        _reason_if(F.col("quantity") <= 0, "INVALID_QUANTITY"),
        _reason_if(F.col("unit_price") < 0, "INVALID_UNIT_PRICE"),
        _reason_if(F.col("discount_amount") < 0, "INVALID_DISCOUNT"),
        _reason_if(~F.col("status").isin(VALID_STATUSES), "INVALID_STATUS"),
        _reason_if(F.col("is_duplicate_order_id"), "DUPLICATE_ORDER_ID"),
    )
    return (
        silver_orders.withColumn("reason_codes", reasons)
        .withColumn("quality_status", F.when(F.size("reason_codes") == 0, "PASS").otherwise("FAIL"))
        .withColumn(
            "order_amount",
            F.round((F.col("quantity") * F.col("unit_price")) - F.col("discount_amount"), 2),
        )
    )


def passed_orders(checked_orders: DataFrame) -> DataFrame:
    """Return records that satisfy all row-level quality gates."""
    return checked_orders.filter(F.col("quality_status") == "PASS")


def quarantined_orders(checked_orders: DataFrame, pipeline_run_id: str) -> DataFrame:
    """Return failed records with reason codes for quarantine storage."""
    return (
        checked_orders.filter(F.col("quality_status") == "FAIL")
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("quarantined_at", F.current_timestamp())
    )


def quality_metrics_summary(
    checked_orders: DataFrame,
    quarantine_df: DataFrame,
    schema_checks: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Generate pass rate, failure counts by check type, and trendable run metrics."""
    spark = checked_orders.sparkSession
    total_count = checked_orders.count()
    failed_count = quarantine_df.count()
    passed_count = total_count - failed_count
    pass_rate = passed_count / total_count if total_count else 1.0

    run_rows = [
        (
            pipeline_run_id,
            "row_quality",
            "PASS" if failed_count == 0 else "FAIL",
            total_count,
            failed_count,
            pass_rate,
            "Record-level gates before Gold",
        )
    ]
    run_metrics = spark.createDataFrame(
        run_rows,
        "pipeline_run_id string, check_type string, status string, records_checked long, "
        "failure_count long, pass_rate double, details string",
    )

    reason_metrics = (
        quarantine_df.select(F.explode("reason_codes").alias("check_type"))
        .groupBy("check_type")
        .count()
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("status", F.lit("FAIL"))
        .withColumn("records_checked", F.lit(total_count).cast("long"))
        .withColumnRenamed("count", "failure_count")
        .withColumn("pass_rate", F.lit(pass_rate).cast("double"))
        .withColumn("details", F.lit("Failure count by reason code"))
        .select(run_metrics.columns)
    )

    schema_metrics = (
        schema_checks.groupBy("check_type", "status")
        .count()
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("records_checked", F.lit(total_count).cast("long"))
        .withColumnRenamed("count", "failure_count")
        .withColumn(
            "pass_rate",
            F.when(F.col("status") == "PASS", F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn("details", F.lit("Schema contract checks"))
        .select(run_metrics.columns)
    )

    return (
        run_metrics.unionByName(reason_metrics, allowMissingColumns=True)
        .unionByName(schema_metrics, allowMissingColumns=True)
        .withColumn("metric_date", F.current_date())
        .withColumn("created_at", F.current_timestamp())
    )


def expected_order_schema() -> dict[str, str]:
    """Expose the expected Silver schema for tests and notebooks."""
    return dict(EXPECTED_ORDER_COLUMNS)


def expected_raw_order_schema() -> dict[str, str]:
    """Expose the expected raw order contract used to detect producer drift."""
    return {
        "order_id": "string",
        "customer_id": "string",
        "order_ts": "string",
        "region": "string",
        "product_id": "string",
        "quantity": "bigint",
        "unit_price": "double",
        "discount_amount": "double",
        "status": "string",
    }


def required_order_columns() -> list[str]:
    """Expose required business columns for documentation and tests."""
    return list(REQUIRED_COLUMNS)
