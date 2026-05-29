"""Gold business aggregates built only from quality-approved orders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_order_volume(orders: DataFrame) -> DataFrame:
    """Aggregate daily order counts and revenue by order date."""
    return (
        orders.withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("order_amount"), 2).alias("gross_revenue"),
            F.countDistinct("customer_id").alias("active_customers"),
        )
        .orderBy("order_date")
    )


def revenue_by_region(orders: DataFrame) -> DataFrame:
    """Aggregate revenue and units by region for analytics consumers."""
    return (
        orders.groupBy("region")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("order_amount"), 2).alias("gross_revenue"),
        )
        .orderBy(F.desc("gross_revenue"))
    )

