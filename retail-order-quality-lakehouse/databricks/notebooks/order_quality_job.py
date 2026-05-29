# Databricks notebook source
"""Databricks Workflow entry point for the retail order quality lakehouse."""

from __future__ import annotations

from datetime import datetime, timezone

from order_quality.bronze import read_raw_orders
from order_quality.gold import daily_order_volume, revenue_by_region
from order_quality.quality import (
    apply_order_quality_checks,
    expected_order_schema,
    expected_raw_order_schema,
    passed_orders,
    quality_metrics_summary,
    quarantined_orders,
    schema_drift_checks,
)
from order_quality.silver import standardize_orders


dbutils.widgets.text("task", "all")
dbutils.widgets.text("raw_path", "dbfs:/retail-quality/sample_data")
dbutils.widgets.text("schema_name", "retail_quality")
dbutils.widgets.text("pipeline_run_id", "")

task = dbutils.widgets.get("task")
raw_path = dbutils.widgets.get("raw_path")
schema_name = dbutils.widgets.get("schema_name")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") or datetime.now(timezone.utc).strftime(
    "%Y%m%d%H%M%S"
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def write_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{schema_name}.{table_name}"
    )


def run_bronze() -> None:
    bronze_orders = read_raw_orders(spark, f"{raw_path}/orders/*.json")
    write_table(bronze_orders, "bronze_orders")


def run_silver() -> None:
    bronze_orders = spark.table(f"{schema_name}.bronze_orders")
    silver_orders = standardize_orders(bronze_orders)
    raw_schema_checks = schema_drift_checks(bronze_orders, expected_raw_order_schema())
    silver_schema_checks = schema_drift_checks(silver_orders, expected_order_schema())
    write_table(silver_orders, "silver_orders")
    write_table(raw_schema_checks.unionByName(silver_schema_checks), "schema_quality_checks")


def run_quality() -> None:
    silver_orders = spark.table(f"{schema_name}.silver_orders")
    schema_checks = spark.table(f"{schema_name}.schema_quality_checks")
    checked_orders = apply_order_quality_checks(silver_orders)
    clean_orders = passed_orders(checked_orders)
    quarantine = quarantined_orders(checked_orders, pipeline_run_id)
    metrics = quality_metrics_summary(checked_orders, quarantine, schema_checks, pipeline_run_id)
    write_table(clean_orders, "silver_orders_valid")
    write_table(quarantine, "quarantine_orders")
    write_table(metrics, "quality_metrics_summary")


def run_gold() -> None:
    valid_orders = spark.table(f"{schema_name}.silver_orders_valid")
    write_table(daily_order_volume(valid_orders), "gold_daily_order_volume")
    write_table(revenue_by_region(valid_orders), "gold_revenue_by_region")


if task in ("bronze", "all"):
    run_bronze()
if task in ("silver", "all"):
    run_silver()
if task in ("quality", "all"):
    run_quality()
if task in ("gold", "all"):
    run_gold()

