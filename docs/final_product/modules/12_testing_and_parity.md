# Module Spec: Testing and Parity

## Purpose

Protect correctness during migration by validating that the SQLite-backed application matches the current motorcycle reporting pipeline closely enough for production use.

This module is cross-cutting and validates frontend, backend, and data-layer behavior.

## Responsibilities

- define parity checks against the current pipeline
- add regression tests for critical logic
- validate report outputs and operational flows

## Functional Scope

- unit tests
- integration tests
- parity comparison scripts
- golden-snapshot checks for selected outputs
- operator workflow smoke tests

## Inputs

- current pipeline outputs
- migrated SQL-backed outputs
- representative source CSVs and reference files

## Outputs

- test suite
- parity report
- release readiness checks

## Communication With Other Modules

- validates behavior across `ingestion`, `reference_data`, `transformation`, `reporting_marts`, `exports`, and `observability_and_run_control`
- informs go/no-go decisions for `packaging_and_distribution`

## Layer Boundary

- This module is not owned by one runtime layer.
- It validates the contract between data, backend, and frontend concerns.
- It should remain independent enough to catch regressions across module boundaries.

## Key Design Constraints

- parity work must focus on business-critical metrics, not only row counts
- tests should remain stable as weekly data changes
- operator-facing workflows need at least basic smoke coverage

## Main Deliverables

- test plan
- parity acceptance criteria
- automated regression checks
- manual validation checklist for report releases

## Open Decisions

- exact tolerance thresholds for report parity
- which generated outputs are checked byte-for-byte vs metric-for-metric

## Phase-1 Acceptance Baseline

Current backend acceptance baseline:

- SQL-backed row counts for silver latest snapshot and all current gold marts must match the legacy file outputs
- core metric parity must hold for:
  - `gold_brand_weekly`
  - `gold_price_positioning_weekly`
  - `gold_recap_by_brand_weekly`
- a headless weekly run must:
  - finish successfully
  - write a `pipeline_runs` record
  - produce expected report files
  - write a run log

Implemented assets:

- parity helper module in `src/moto_app/testing/parity.py`
- manual parity tool in `database/tools/check_parity.py`
- integration test suite in `tests/test_backend_pipeline.py`

## Task Checklist

- [x] define parity acceptance criteria for migration
- [x] define row-count and metric-level validation checks
- [x] add tests for ingestion, transformation, marts, and exports
- [x] define representative fixtures and sample-run datasets
- [x] create a regression checklist for report generation
- [ ] define smoke coverage for operator workflows
- [x] document release-readiness gates before packaging
