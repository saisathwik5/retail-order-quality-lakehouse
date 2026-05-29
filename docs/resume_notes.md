# Resume Notes

## Project Pitch

Production-style Databricks data quality project that demonstrates governance
and reliability before records reach Gold tables. It uses retail order data to
make business outputs easy to understand while keeping the engineering focus on
quality gates, quarantine, metrics, and orchestration.

## Talking Points

- Explain why quarantine is better than dropping bad rows: it preserves evidence
  for producer fixes and reprocessing.
- Point out that schema drift is separated from row-level validation because it
  is often a producer contract issue, not a single-record issue.
- The metrics table is trendable by `metric_date` and `pipeline_run_id`, which
  makes it suitable for Databricks SQL alerts or dashboarding.
- Gold tables are intentionally downstream of quality gates, making the business
  output trustworthy.

