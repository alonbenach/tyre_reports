# Module Spec: Operator UI

## Purpose

Provide a Windows desktop interface that lets a non-technical user run, monitor, and verify the weekly pipeline safely.

This module is the frontend layer boundary for the product.

## Responsibilities

- guide the weekly run process
- show validation errors in plain language
- display progress and run logs
- expose latest outputs and instructions

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

## Task Checklist

- [x] finalize the UI toolkit choice
- [x] define screen list and navigation structure
- [x] define backend service calls required by each screen
- [x] design the weekly-run workflow and validation display
- [x] design output access and run-history screens
- [x] define instruction and troubleshooting content sources
- [x] confirm that no business logic lives in the UI layer
