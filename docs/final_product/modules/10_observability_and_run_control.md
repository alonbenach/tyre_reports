# Module Spec: Observability and Run Control

## Purpose

Track runs, logs, statuses, failures, and operator-facing diagnostics so the pipeline is supportable when the primary owner is unavailable.

This module is part of the backend/application layer.

## Responsibilities

- create and update run records
- centralize application logging
- translate technical errors into operator-safe messages
- support rerun and recovery logic

## Functional Scope

- run lifecycle management
- structured logs
- status events
- error classification
- support diagnostics

## Inputs

- lifecycle events from all application modules
- exceptions and warnings
- run settings and timestamps

## Outputs

- `pipeline_runs` status updates
- text and structured logs
- operator-readable error messages
- support and debug diagnostics

## Communication With Other Modules

- receives events from `app_shell`, `ingestion`, `transformation`, `reporting_marts`, `exports`, and `reference_data`
- exposes current run state to `operator_ui`
- persists metadata through `storage_and_migrations`

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns run-state and diagnostic services, not UI rendering and not DB schema ownership.
- It may persist logs and run metadata through the data layer.

## Key Design Constraints

- logs must support both operator use and developer debugging
- failed runs should be easy to identify and diagnose
- rerun behavior must be explicit and safe

## Main Deliverables

- run registry service
- logging strategy
- error translation layer
- status event model

## Locked Decisions

- Every weekly run must create a run record before data-changing work begins.
- Phase-1 runs remain manually triggered.
- Run status should be persisted in SQLite and mirrored to disk logs for supportability.
- Logs should exist both on disk and in SQL-backed run metadata at a summary level.
- Partial reruns are out of scope for v1; rerun behavior should operate at the weekly run or snapshot level.
- Errors should be translated into operator-safe summaries while preserving technical detail in support logs.
- Human-readable failure summaries are required across ingestion, reference data, transformation, marts, and exports, not only in the future UI layer.

## Run Lifecycle Model

Planned baseline statuses:

- `pending`
- `running`
- `failed`
- `succeeded`
- `cancelled` if cancellation is later supported

Each run should track at least:

- run id
- snapshot date
- started timestamp
- finished timestamp
- status
- source file identity
- report mode
- PDF mode
- failure summary if applicable

## Logging Policy

Planned logging split:

- disk logs for detailed traces and support diagnostics
- SQLite-backed run metadata for high-level run history and UI status display

Phase-1 implementation notes:

- weekly runs are recorded in `pipeline_runs`
- run logs are written to `logs/<run_id>.log`
- backend consumers can query recent run state through the observability service rather than reading SQL directly
- low-level CLI tools print operator-safe summaries before exiting non-zero
- the weekly coordinator logs stage-level timings and concise row-count summaries for ingestion, transformation, marts, and exports

Planned backend package layout:

```text
src/
  moto_app/
    observability/
```

## Remaining Open Decisions

- exact log retention policy
- exact error taxonomy for operator-facing translation
- whether phase-1 operator-safe errors should be implemented as shared exception classes, wrapper helpers per module, or both

## Task Checklist

- [x] define the run lifecycle model and statuses
- [x] define logging format and storage policy
- [x] implement run record creation and completion/failure updates
- [x] define error classes and operator-safe message translation
- [x] ensure ingestion, reference-data, transformation, marts, and export flows all emit human-readable failure summaries
- [x] define rerun and recovery behavior
- [x] expose current and historical run state to backend consumers and UI
- [x] document diagnostics needed for support handoff
