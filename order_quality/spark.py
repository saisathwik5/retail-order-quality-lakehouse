"""Spark session helpers for local and Databricks runs."""

from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark(app_name: str = "retail-order-quality") -> SparkSession:
    """Create a local Spark session with deterministic settings for tests and demos."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

