-- Business analytics layer over Gold tables.
-- Replace `retail_quality` with your catalog/schema if you deploy under Unity Catalog.

SELECT
  region,
  order_count,
  units_sold,
  gross_revenue
FROM retail_quality.gold_revenue_by_region
ORDER BY gross_revenue DESC;

SELECT
  order_date,
  order_count,
  active_customers,
  gross_revenue
FROM retail_quality.gold_daily_order_volume
ORDER BY order_date;

SELECT
  metric_date,
  check_type,
  status,
  failure_count,
  pass_rate
FROM retail_quality.quality_metrics_summary
ORDER BY metric_date DESC, check_type;
