# Module Spec: Operator UI

## Purpose

Provide a Windows desktop interface that lets a non-technical user run, monitor, and verify the weekly pipeline safely.

This module is the frontend layer boundary for the product.

## Responsibilities

- guide the weekly run process
- show validation errors in plain language
- display progress and run logs
- expose latest outputs and instructions
- allow operators to provide the weekly CSV by drag-and-drop or file browsing

## Functional Scope

- home/status screen
- weekly run screen
- run log/history view
- outputs view
- instructions/troubleshooting view
- admin/reference refresh view

## Inputs

- operator actions
- run status events
- latest output metadata
- configuration and instruction content

## Outputs

- run requests
- admin refresh requests
- UI state for logs, validation, and outputs

## Communication With Other Modules

- launches actions through `app_shell`
- reads latest state from `storage_and_migrations` and `observability_and_run_control`
- triggers `ingestion`, `exports`, and `reference_data` workflows indirectly

## Layer Boundary

- This module owns presentation and operator interaction only.
- It should not contain business transformation logic.
- It should not execute direct SQL writes outside backend services.
- Any database reads used for UI display should flow through backend-facing query or service interfaces.

## Key Design Constraints

- UI must be usable by someone with no Python or SQL knowledge
- critical errors must be understandable without stack traces
- the app should make the correct weekly path obvious

## Main Deliverables

- desktop UI shell
- weekly-run workflow
- output access panel
- embedded instructions and troubleshooting content

## Open Decisions

- whether admin features ship in v1 or later

## Phase-1 Implementation Notes

Current UI implementation:

- toolkit: `PySide6`
- entrypoint: `database/tools/launch_ui.py`
- code area:

```text
src/
  moto_app/
    ui/
```

Current screens:

- `Home`
- `Weekly Run`
- `Run History`
- `Outputs`
- `Instructions`

Current backend-facing UI dependencies:

- `run_weekly_pipeline(...)`
- `latest_run_status(...)`
- `list_runs(...)`
- `list_generated_reports(...)`

The current UI polls backend state and log files and does not implement business logic locally.

Current operator conveniences:

- home screen coverage cards for current-year and previous-year loaded weeks
- drag-and-drop CSV input in the weekly run screen
- dropped or browsed CSV files are staged into `data/ingest/` for controlled app intake
- operators choose the snapshot date in the UI, and the staged intake file is renamed to match that date
- the chosen snapshot date is shown explicitly, and the staged filename/path are displayed before the run starts
- operators can rerun a snapshot directly from an already staged intake file for the selected date
- staging/naming and run selection are separate steps so the date picker does not silently change the run target
- duplicate snapshot conflicts should be surfaced before the backend run starts when replace mode is off
- the outputs tab should show the current live files for the selected report, not every historical generation event
- report-specific output browsing
- direct opening of per-report `excel` and `reports` folders

## Task Checklist

- [x] finalize the UI toolkit choice
- [x] define screen list and navigation structure
- [x] define backend service calls required by each screen
- [x] design the weekly-run workflow and validation display
- [x] design output access and run-history screens
- [x] define instruction and troubleshooting content sources
- [x] confirm that no business logic lives in the UI layer
- [x] show compact DB week-coverage status on the home screen for current and previous year
- [x] support drag-and-drop CSV selection for weekly runs
- [x] stage operator-selected CSV files into a controlled intake folder
- [x] support operator-controlled snapshot-date renaming for staged CSV intake
- [x] make the selected snapshot date and staged filename/path explicit before the run starts
- [x] allow reruns from an existing staged intake file without re-dropping the CSV
- [x] separate staging/naming from the staged snapshot chosen for the run
- [x] warn about duplicate snapshot conflicts before starting a run when replace mode is off
- [x] show current live output files rather than the full generated-output audit history
- [x] support report-specific output browsing with separate Excel and reports folders
