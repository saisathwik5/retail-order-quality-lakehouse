"""Silver transformations for typed, standardized retail orders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


EXPECTED_ORDER_COLUMNS = {
    "order_id": "string",
    "customer_id": "string",
    "order_ts": "timestamp",
    "region": "string",
    "product_id": "string",
    "quantity": "int",
    "unit_price": "double",
    "discount_amount": "double",
    "status": "string",
}

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_ts",
    "region",
    "product_id",
    "quantity",
    "unit_price",
    "status",
]


def _column_or_null(df: DataFrame, column_name: str) -> F.Column:
    if column_name in df.columns:
        return F.col(column_name)
    return F.lit(None)


def _try_cast_or_null(df: DataFrame, column_name: str, target_type: str) -> F.Column:
    if column_name in df.columns:
        return F.expr(f"try_cast(`{column_name}` as {target_type})")
    return F.lit(None).cast(target_type)


def standardize_orders(bronze_orders: DataFrame) -> DataFrame:
    """Cast raw Bronze records into the expected Silver order contract."""
    typed = bronze_orders.select(
        _column_or_null(bronze_orders, "order_id").cast("string").alias("order_id"),
        _column_or_null(bronze_orders, "customer_id").cast("string").alias("customer_id"),
        _try_cast_or_null(bronze_orders, "order_ts", "timestamp").alias("order_ts"),
        F.upper(_column_or_null(bronze_orders, "region").cast("string")).alias("region"),
        _column_or_null(bronze_orders, "product_id").cast("string").alias("product_id"),
        _try_cast_or_null(bronze_orders, "quantity", "int").alias("quantity"),
        _try_cast_or_null(bronze_orders, "unit_price", "double").alias("unit_price"),
        F.coalesce(
            _try_cast_or_null(bronze_orders, "discount_amount", "double"),
            F.lit(0.0),
        ).alias("discount_amount"),
        F.upper(_column_or_null(bronze_orders, "status").cast("string")).alias("status"),
        F.col("_source_file"),
        F.col("_ingestion_ts"),
    )
    duplicate_window = Window.partitionBy("order_id")
    return typed.withColumn("is_duplicate_order_id", F.count("*").over(duplicate_window) > 1)
