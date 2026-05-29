from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from order_quality.bronze import read_raw_orders
from order_quality.gold import daily_order_volume, revenue_by_region
from order_quality.quality import apply_order_quality_checks, passed_orders
from order_quality.silver import standardize_orders


def test_gold_uses_only_quality_approved_orders(spark):
    input_path = Path(__file__).resolve().parents[1] / "sample_data" / "orders" / "*.json"
    bronze = read_raw_orders(spark, str(input_path))
    clean = passed_orders(apply_order_quality_checks(standardize_orders(bronze)))

    daily = daily_order_volume(clean)
    regional = revenue_by_region(clean)

    assert clean.filter(F.col("quality_status") == "FAIL").count() == 0
    assert daily.agg(F.sum("order_count").alias("orders")).first()["orders"] == clean.count()
    assert regional.filter(F.col("region") == "NORTHEAST").count() == 1
