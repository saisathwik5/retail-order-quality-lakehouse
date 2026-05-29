from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from order_quality.bronze import read_raw_orders
from order_quality.quality import (
    apply_order_quality_checks,
    expected_raw_order_schema,
    quality_metrics_summary,
    quarantined_orders,
    schema_drift_checks,
)
from order_quality.silver import standardize_orders


def test_quality_checks_quarantine_expected_failures(spark):
    input_path = Path(__file__).resolve().parents[1] / "sample_data" / "orders" / "*.json"
    bronze = read_raw_orders(spark, str(input_path))
    silver = standardize_orders(bronze)
    checked = apply_order_quality_checks(silver)
    quarantine = quarantined_orders(checked, "test-run")

    reasons = {
        row["reason"]
        for row in quarantine.select(F.explode("reason_codes").alias("reason")).collect()
    }

    assert "NULL_CUSTOMER_ID" in reasons
    assert "NULL_OR_INVALID_ORDER_TS" in reasons
    assert "INVALID_QUANTITY" in reasons
    assert "INVALID_STATUS" in reasons
    assert "DUPLICATE_ORDER_ID" in reasons


def test_schema_drift_detects_extra_raw_column(spark):
    input_path = Path(__file__).resolve().parents[1] / "sample_data" / "orders" / "*.json"
    bronze = read_raw_orders(spark, str(input_path))
    checks = schema_drift_checks(bronze, expected_raw_order_schema())

    drift_rows = checks.filter(
        (F.col("check_type") == "schema_extra_column") & (F.col("column_name") == "promo_code")
    ).collect()

    assert drift_rows
    assert drift_rows[0]["status"] == "WARN"


def test_quality_metrics_include_pass_rate_and_failure_counts(spark):
    input_path = Path(__file__).resolve().parents[1] / "sample_data" / "orders" / "*.json"
    bronze = read_raw_orders(spark, str(input_path))
    silver = standardize_orders(bronze)
    checked = apply_order_quality_checks(silver)
    quarantine = quarantined_orders(checked, "test-run")
    schema_checks = schema_drift_checks(bronze, expected_raw_order_schema())

    metrics = quality_metrics_summary(checked, quarantine, schema_checks, "test-run")
    row_quality = metrics.filter(F.col("check_type") == "row_quality").first()
    duplicate_metric = metrics.filter(F.col("check_type") == "DUPLICATE_ORDER_ID").first()

    assert row_quality["failure_count"] > 0
    assert 0 < row_quality["pass_rate"] < 1
    assert duplicate_metric["failure_count"] == 2

