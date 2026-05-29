# Architecture Notes

## Medallion Layers

Bronze stores raw order events with ingestion metadata. Silver enforces a typed
contract and adds duplicate flags. The quality gate splits Silver into approved
records and quarantine records. Gold is built only from approved records.

## Quality Framework

The quality framework is intentionally small and easy to audit:

- `schema_drift_checks` compares actual columns and types with an expected
  producer contract and Silver contract.
- `apply_order_quality_checks` appends row-level `reason_codes`.
- `quarantined_orders` preserves failed records with `pipeline_run_id` and
  `quarantined_at`.
- `quality_metrics_summary` emits pass rate and failure counts by check type.

## Workflow Design

The Databricks job uses four dependent tasks:

1. `bronze_ingest`
2. `silver_standardize_and_schema_checks`
3. `quality_gate_and_quarantine`
4. `gold_business_outputs`

Each task has retry settings, and Gold depends on the quality gate so failed
records cannot silently flow into business-facing tables.

