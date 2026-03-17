# Module Spec: Transformation

## Purpose

Apply motorcycle-specific filtering, normalization, enrichment, canonical matching, and silver-layer calculations.

This module is part of the backend/application layer.

## Responsibilities

- filter source data to motorcycle scope
- normalize textual and numeric fields
- derive reporting attributes
- join reference datasets
- produce the SQL-backed silver layer

## Functional Scope

- motorcycle filtering
- production-year filtering
- seller, brand, size, and name normalization
- price and stock conversions
- rim and fitment derivations
- canonical mapping and campaign enrichment
- deduplication and silver persistence

## Inputs

- staged source data from `ingestion`
- reference tables from `reference_data`
- settings from `configuration`

## Outputs

- `silver_motorcycle_weekly`
- row counts and quality metrics
- transformation warnings/errors

## Communication With Other Modules

- invoked by `app_shell`
- reads and writes via `storage_and_migrations`
- depends on `reference_data`
- supplies datasets to `reporting_marts`
- emits metrics to `observability_and_run_control`

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns business transformation logic.
- It should not contain UI code and should not own raw database infrastructure concerns.

## Key Design Constraints

- preserve current business logic parity wherever possible
- support incremental processing by snapshot
- keep transformation logic testable outside the UI

## Main Deliverables

- transformation service
- silver table writer
- data quality checks
- parity checklist against the current file-based pipeline

## Locked Decisions

- Phase 1 remains motorcycle-only.
- The migrated transformation flow should preserve current business logic before attempting optimization.
- Snapshot-scoped incremental processing is the target design, even if an initial implementation temporarily rebuilds motorcycle scope for validation.
- Most business transformation logic should remain in Python services in phase 1.
- SQL should primarily hold persisted results and simple read views, not absorb complex matching logic immediately.
- The silver layer should remain the stable backend contract between transformation and reporting marts.

## Current Transformation Scope To Preserve

The SQL-backed migration should preserve the existing behavior for:

- motorcycle filtering
- production-year filtering to current year and previous year
- text normalization for brand, seller, size, and name
- numeric conversion for price, stock, and rim values
- rim grouping
- fitment-position inference from product name
- pattern-family derivation
- canonical matching
- campaign and discount enrichment
- duplicate handling on the current business key

## Silver-Layer Role

The silver layer should store:

- one normalized motorcycle dataset per snapshot
- all columns required for downstream marts and report generation
- enrichment outputs needed for canonical quality, discounts, fitment, and recap logic
- run-linked metadata sufficient for debugging and parity checks

Planned backend package layout:

```text
src/
  moto_app/
    transform/
```

## Remaining Open Decisions

- how much transformation logic to keep in Python vs SQL
- exact silver-level data quality thresholds that should fail a run
- whether the first migrated release writes snapshot partitions by delete-and-reload or append-with-replace semantics

## Task Checklist

- [x] map current transformation steps to the SQL-backed target design
- [x] define silver-layer schema
- [x] implement normalization and derived-field logic
- [x] implement canonical matching against SQL-backed reference tables
- [x] implement deduplication and production-year filters
- [x] define snapshot processing strategy
- [ ] define silver-level data quality checks and failure thresholds
- [ ] define what transformation metrics are recorded per run for diagnostics
- [ ] validate parity of silver outputs against the current pipeline
