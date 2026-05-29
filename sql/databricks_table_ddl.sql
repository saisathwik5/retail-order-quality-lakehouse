CREATE SCHEMA IF NOT EXISTS retail_quality;

CREATE TABLE IF NOT EXISTS retail_quality.bronze_orders
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.silver_orders
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.silver_orders_valid
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.quarantine_orders
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.schema_quality_checks
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.quality_metrics_summary
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.gold_daily_order_volume
USING DELTA;

CREATE TABLE IF NOT EXISTS retail_quality.gold_revenue_by_region
USING DELTA;
