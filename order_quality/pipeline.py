"""End-to-end local runner for the retail order data quality lakehouse."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from order_quality.bronze import read_raw_orders, write_bronze
from order_quality.gold import daily_order_volume, revenue_by_region
from order_quality.quality import (
    apply_order_quality_checks,
    expected_raw_order_schema,
    expected_order_schema,
    passed_orders,
    quality_metrics_summary,
    quarantined_orders,
    schema_drift_checks,
)
from order_quality.silver import standardize_orders
from order_quality.spark import create_spark


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def run_pipeline(input_dir: str | Path, warehouse_dir: str | Path, fmt: str = "parquet") -> None:
    """Run Bronze, Silver, quality gates, quarantine, metrics, and Gold outputs."""
    input_path = Path(input_dir)
    warehouse_path = Path(warehouse_dir)
    pipeline_run_id = _run_id()
    spark = create_spark()

    bronze_orders = read_raw_orders(spark, str(input_path / "orders" / "*.json"))
    silver_orders = standardize_orders(bronze_orders)
    raw_schema_checks = schema_drift_checks(bronze_orders, expected_raw_order_schema())
    silver_schema_checks = schema_drift_checks(silver_orders, expected_order_schema())
    schema_checks = raw_schema_checks.unionByName(silver_schema_checks)
    checked_orders = apply_order_quality_checks(silver_orders)
    clean_orders = passed_orders(checked_orders)
    quarantine = quarantined_orders(checked_orders, pipeline_run_id)
    metrics = quality_metrics_summary(checked_orders, quarantine, schema_checks, pipeline_run_id)
    gold_daily = daily_order_volume(clean_orders)
    gold_region = revenue_by_region(clean_orders)

    write_bronze(bronze_orders, str(warehouse_path / "bronze" / "orders"), fmt)
    silver_orders.write.format(fmt).mode("overwrite").save(str(warehouse_path / "silver" / "orders"))
    clean_orders.write.format(fmt).mode("overwrite").save(str(warehouse_path / "silver" / "orders_valid"))
    quarantine.write.format(fmt).mode("overwrite").save(str(warehouse_path / "quarantine" / "orders"))
    metrics.write.format(fmt).mode("overwrite").save(str(warehouse_path / "quality" / "metrics_summary"))
    gold_daily.write.format(fmt).mode("overwrite").save(str(warehouse_path / "gold" / "daily_order_volume"))
    gold_region.write.format(fmt).mode("overwrite").save(str(warehouse_path / "gold" / "revenue_by_region"))

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the retail order quality lakehouse.")
    parser.add_argument("--input", default="sample_data", help="Input sample data directory.")
    parser.add_argument("--warehouse", default="lakehouse", help="Output warehouse directory.")
    parser.add_argument("--format", default="parquet", choices=["parquet", "delta"], help="Output format.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.input, args.warehouse, args.format)
