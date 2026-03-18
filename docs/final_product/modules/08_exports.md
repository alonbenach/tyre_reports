# Module Spec: Exports

## Purpose

Generate management-facing Excel and PDF deliverables from SQL-backed report datasets.

This module is part of the backend/application layer.

## Responsibilities

- read report-ready data from SQLite
- generate Excel workbooks
- generate PDF reports
- record output metadata and file paths

## Functional Scope

- positioning report export
- offeror-focus report export
- optional future ad hoc Excel exports from selected datasets
- output file naming and overwrite policy

## Inputs

- report datasets from `reporting_marts`
- report and output settings from `configuration`
- run context from `app_shell`

## Outputs

- Excel files in report-specific `reports/<report_name>/excel/`
- PDF files in report-specific `reports/<report_name>/reports/`
- `generated_reports` metadata entries

## Communication With Other Modules

- called by `app_shell`
- reads data via `storage_and_migrations` or `reporting_marts`
- reports status to `observability_and_run_control`
- surfaced in `operator_ui`

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns generation of deliverable files, not presentation-layer behavior.
- It may read SQL-backed datasets, but it should not own core schema design.

## Key Design Constraints

- outputs are deliverables only, not dependencies
- file names should remain familiar to business users
- report families and file formats should be separated into predictable folders for operator browsing
- export failures should not corrupt database state

## Main Deliverables

- SQL-backed report adapters
- output writer services
- generated file registry

## Locked Decisions

- Excel and PDF outputs remain deliverables only, not pipeline dependencies.
- The first export goal is parity with the current two report families:
  - price positioning report
  - offeror-focus report
- Export services should read SQL-backed marts and silver-backed service data through backend adapters rather than directly from editable files.
- Output filenames should remain familiar to current business users.
- PDF generation remains optional in phase 1.
- Generated files must be recorded in output metadata for traceability.
- UI output browsing should reflect the current live files on disk, while full generation history remains in metadata for audit.
- Recap and other presentation strings should be formatted in the export layer, not stored as the source of truth in the DB.
- The SQL-backed export path should inject reference datasets into legacy report modules instead of letting report internals load spreadsheets directly.
- Output layout should support `Outputs -> report name -> reports and excel folder` in the UI and on disk.
- Legacy flat report files in `reports/` should be normalized into the canonical per-report folder layout when exports run.
- A weekly run must export against that run's target snapshot date, not simply the maximum snapshot present in the database.

## Current Export Scope To Preserve

The backend export layer should preserve equivalent deliverables for:

- `PRICE_POSITIONING_Wxx_Poland.xlsx`
- `PRICE_POSITIONING_Wxx_Poland.pdf`
- `offeror_focus_Wxx_Poland.xlsx`
- `offeror_focus_Wxx_Poland.pdf`

Expected folder layout:

```text
reports/
  price_positioning/
    excel/
    reports/
  offeror_focus/
    excel/
    reports/
```

## Export Input Contract

The exports layer should consume:

- SQL-backed gold datasets for report sections already materialized in marts
- SQL-backed latest views where a current-week slice is needed
- SQL-backed silver access where the current report logic still requires lower-level detail
- SQL-backed reference datasets for canonical mapping and campaign discount lookups still needed by report internals

Planned backend package layout:

```text
src/
  moto_app/
    exports/
```

## Remaining Open Decisions

- whether future ad hoc export-selected-table-to-Excel belongs in backend export services or a UI-triggered wrapper
- exact overwrite policy when the same report filename already exists

## Task Checklist

- [x] define report input contracts from SQL-backed marts
- [x] adapt positioning export to read from SQL-backed datasets
- [x] adapt offeror-focus export to read from SQL-backed datasets
- [x] define output naming and overwrite rules
- [x] persist generated-output metadata
- [x] remove remaining workbook-based reference reads from report internals for the SQL-backed export path
- [x] define operator-facing export error messages
- [x] decide how optional PDF generation is handled in packaged runtime
- [x] define report-family output folder structure for Excel and PDF deliverables
