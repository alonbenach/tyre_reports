# Module Spec: Storage and Migrations

## Purpose

Provide SQLite persistence, schema management, transaction control, and database access patterns for the application.

This module is the data/DB layer boundary for the product.

## Responsibilities

- own the SQLite schema
- apply schema migrations
- manage connections and transaction boundaries
- expose repositories or query services for other modules

## Functional Scope

- SQLite connection management
- migration runner
- run metadata persistence
- import, silver, gold, and reference table access
- query views for current reports

## Inputs

- database path from `configuration`
- migration files from `database/migrations/`
- write requests from ingestion, transformation, marts, and observability services

## Outputs

- durable tables and views
- migration history
- transactional guarantees for weekly runs

## Communication With Other Modules

- used by every data-producing and data-reading module
- stores state for `ingestion`, `reference_data`, `transformation`, `reporting_marts`, `exports`, and `observability_and_run_control`

## Layer Boundary

- This module owns persistence concerns only.
- It should not contain report business logic or UI behavior.
- Frontend code should not access SQLite directly; access should flow through backend services.

## Key Design Constraints

- no processed runtime dependency should rely on editable Excel files
- every weekly run must be auditable
- failed runs must not leave partial state in gold outputs
- schema changes must be repeatable and controlled

## Main Deliverables

- SQLite schema
- `schema_migrations` tracking table
- repository/query layer
- transaction utilities
- seed/bootstrap logic

## Locked Decisions

- SQLite is the only phase-1 processed-data store.
- Migration files will live in `database/migrations/` and be applied in ordered sequence.
- The migration runner will record applied versions in `schema_migrations`.
- The backend will use a lightweight query-service/repository approach rather than introducing a heavy ORM.
- Frontend code will not talk directly to SQLite.
- Write flows will be orchestrated by backend services and committed through explicit transaction boundaries.
- Report exports will read SQL-backed tables or views only.
- `run_id` is audit lineage only and must not be used to make business data unique.
- Business data uniqueness is defined by `snapshot_date` plus the table's business grain.
- Same-snapshot reruns must replace that snapshot transactionally in staging, silver, and gold layers rather than append duplicate business rows.
- Reports must not deduplicate by `run_id`; they must read one active dataset per `snapshot_date`.

## Migration File Convention

Planned naming pattern:

```text
database/
  migrations/
    001_initial_schema.sql
    002_reference_tables.sql
    003_harden_metadata_schema.sql
    004_refine_reference_tables.sql
    005_source_and_stage_tables.sql
    006_silver_motorcycle.sql
    007_gold_tables.sql
    008_latest_views.sql
```

Rules:

- migrations are append-only
- each migration has one ordered numeric prefix
- applied migrations are recorded in `schema_migrations`
- startup should fail fast if migrations cannot be applied cleanly

## Initial Schema Scope

- `schema_migrations`
- `pipeline_runs`
- `source_imports`
- `reference_refresh_runs`
- `ref_*`
- `stg_weekly_source_motorcycle`
- `silver_motorcycle_weekly`
- `gold_*`
- `generated_reports`

## Initial Metadata Tables

`schema_migrations`
- `version`
- `applied_at_utc`

`pipeline_runs`
- `run_id`
- `snapshot_date`
- `run_started_at_utc`
- `run_finished_at_utc`
- `status`
- `report_mode`
- `skip_pdf`
- `source_file_name`
- `source_file_sha256`
- `error_message`
- `app_version`
- `schema_version`

`source_imports`
- `import_id`
- `run_id`
- `snapshot_date`
- `source_file_path`
- `archived_file_path`
- `sha256`
- `row_count_total`
- `row_count_motorcycle`
- `imported_at_utc`

`generated_reports`
- `report_id`
- `run_id`
- `snapshot_date`
- `report_type`
- `format`
- `output_path`
- `generated_at_utc`
- `status`

## Approved Key Strategy

Operational metadata tables:

- surrogate primary keys on `run_id`, `import_id`, `refresh_run_id`, and `report_id`
- foreign keys only for run-lineage relationships

Analytical tables:

- natural composite uniqueness at the snapshot and business grain
- no reliance on `run_id` for business-row uniqueness

Approved lineage foreign keys:

- `source_imports.run_id -> pipeline_runs.run_id`
- `generated_reports.run_id -> pipeline_runs.run_id`

Planned analytical uniqueness rules:

- `silver_motorcycle_weekly`: `snapshot_date + product_code + seller_norm + price_pln`
- `gold_market_weekly`: `snapshot_date`
- `gold_brand_weekly`: `snapshot_date + brand`
- `gold_segment_weekly`: `snapshot_date + analysis_fitment_key + brand`
- `gold_seller_weekly`: `snapshot_date + seller_norm + brand`
- `gold_fitment_weekly`: `snapshot_date + fitment_position + brand + analysis_fitment_key`
- `gold_price_positioning_weekly`: `snapshot_date + granularity + analysis_fitment_key`
- `gold_recap_by_brand_weekly`: `snapshot_date + brand`
- `gold_recap_by_brand_latest`: `snapshot_date + brand`
- `gold_keyfitment_checkpoint_weekly`: `snapshot_date + segment_reference_group + key_fitments + brand + pattern_set + size_root`

## Approved Idempotency Strategy

- multiple `pipeline_runs` may exist for the same `snapshot_date`
- staging, silver, and gold layers should contain one active dataset per `snapshot_date`
- default operator behavior should block duplicate snapshot processing
- explicit backend or admin workflows may support `replace snapshot`
- `replace snapshot` must delete and rebuild one snapshot inside a transaction
- exact same-file reruns should be detected by checksum and blocked by default

## Approved Indexing Strategy

Metadata indexes:

- `pipeline_runs(status, run_started_at_utc)`
- `pipeline_runs(snapshot_date)`
- `source_imports(snapshot_date)`
- `source_imports(sha256)`
- `generated_reports(run_id)`
- `generated_reports(snapshot_date, report_type, format)`

Reference indexes:

- `ref_canonical_fitment_mapping(brand, size_root)`
- `ref_canonical_fitment_mapping(ipcode)`
- `ref_price_list(ipcode)`
- `ref_price_list(brand)`

## Initial Read Views

- `vw_latest_successful_run`
- `vw_latest_positioning_outputs`
- `vw_latest_offeror_outputs`
- `vw_latest_source_import`

## Data Access Pattern

- use SQL scripts plus lightweight Python query services
- keep schema ownership in the DB layer
- keep business calculations in backend services unless a view is clearly simpler and stable
- use views mainly for latest-run and operator-facing read models

## Remaining Open Decisions

- amount of logic held in SQL views vs Python transformations
- whether the DB layer uses raw `sqlite3`, a lightweight helper layer, or `pandas` only at the query edge

## Task Checklist

- [x] define the initial SQLite schema
- [x] define migration file layout and versioning rules
- [x] implement the migration runner
- [x] create connection and transaction helpers
- [x] define repository or query-service access patterns
- [x] create core metadata tables such as `schema_migrations`, `pipeline_runs`, and `generated_reports`
- [x] define initial read views for latest successful outputs
- [x] define indexing strategy for snapshot_date, iso_week, brand, and seller access paths
- [x] define duplicate-run and idempotency behavior at the DB level
