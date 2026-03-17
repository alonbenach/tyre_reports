# Module Spec: Ingestion

## Purpose

Ingest weekly source CSV files, validate them, archive them, and write normalized source-stage records for motorcycle data.

This module is part of the backend/application layer.

## Responsibilities

- validate source CSV presence and schema
- archive raw source files in immutable snapshot folders
- capture row counts and checksums
- load motorcycle-relevant data into SQL staging

## Functional Scope

- source file selection
- filename and snapshot-date handling
- column validation
- duplicate snapshot detection
- raw archive creation
- source import metadata recording

## Inputs

- weekly CSV export from `platformaopon.pl`
- database access
- configuration paths

## Outputs

- archived raw file on disk
- `source_imports` metadata
- staged motorcycle source rows in SQLite

## Communication With Other Modules

- called by `app_shell`
- writes through `storage_and_migrations`
- sends run events to `observability_and_run_control`
- provides staged data for `transformation`
- surfaced to the user through `operator_ui`

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns source validation and import flow, not frontend interaction or DB schema ownership.
- It should expose operator-safe outcomes for the UI without embedding UI code.

## Key Design Constraints

- raw source files should remain immutable after archival
- duplicate imports must be handled predictably
- source validation errors must be translated into operator-safe language

## Main Deliverables

- file validator
- archival service
- import metadata writer
- stage loader

## Locked Decisions

- Weekly runs are manually triggered in phase 1.
- Source ingestion remains CSV-based from `platformaopon.pl`.
- Raw source files will continue to be archived as immutable snapshots under `data/raw/snapshot_date=YYYY-MM-DD/source.csv`.
- Ingestion metadata will move from `ingestion_log.csv` into SQLite-backed metadata tables.
- The ingestion layer will validate the input file before writing stage data.
- Stage loading in phase 1 will keep motorcycle scope only and will load the source columns currently used by the motorcycle pipeline.
- File-level provenance will include at least source path, archived path, checksum, snapshot date, total row count, motorcycle row count, and import timestamp.

## Source Validation Rules

The ingestion layer should validate at least:

- file exists and is readable
- file extension is `.csv`
- filename or selected snapshot date resolves to a valid weekly snapshot token
- all required source columns are present
- file encoding can be read with tolerant decoding
- snapshot duplication is checked before archival and stage writes continue

## Stage Scope

Phase-1 staging scope:

- motorcycle rows only
- source columns currently used by the existing pipeline
- snapshot metadata needed for later silver and gold processing

Planned backend package layout:

```text
src/
  moto_app/
    ingest/
```

## Remaining Open Decisions

- whether the UI should expose `replace snapshot` in v1 or keep it as backend-support functionality only
- exact staging-table column list and typing for the SQL target

## Task Checklist

- [x] define source-file validation rules
- [x] define duplicate snapshot policy
- [x] implement raw file archival strategy
- [x] implement checksum and row-count metadata capture
- [ ] define malformed-file handling and operator retry guidance
- [ ] define whether snapshot date is derived strictly from filename or can be overridden in the UI/backend
- [x] define the staging-table schema for motorcycle source rows
- [x] implement CSV-to-stage loading flow
- [x] expose ingest outcomes in operator-safe terms
