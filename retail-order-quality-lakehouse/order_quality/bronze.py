"""Bronze layer ingestion for raw retail order files."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw_orders(spark: SparkSession, path: str) -> DataFrame:
    """Read semi-structured order JSON and attach ingestion metadata."""
    return (
        spark.read.option("multiLine", "false")
        .json(path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit("retail_orders"))
    )


def write_bronze(df: DataFrame, output_path: str, fmt: str = "parquet") -> None:
    """Persist Bronze data locally as Parquet or in Databricks as Delta."""
    df.write.format(fmt).mode("overwrite").save(output_path)

