# Module Spec: App Shell

## Purpose

Provide the backend application entrypoints, dependency wiring, and execution flow for the final product.

This module is part of the backend/application layer. It is not the frontend UI shell.

## Responsibilities

- define CLI and GUI application entrypoints
- initialize configuration, logging, database access, and services
- expose a single orchestration boundary for weekly runs
- coordinate module startup order

## Functional Scope

- application bootstrap
- dependency container or explicit service wiring
- selection between headless and operator UI modes
- version exposure for logs and support

## Inputs

- app configuration
- runtime mode
- operator-triggered actions

## Outputs

- initialized service graph
- run execution requests
- application status for UI and logs

## Communication With Other Modules

- reads runtime settings from `configuration`
- creates database or session access through `storage_and_migrations`
- delegates runs to `ingestion`, `transformation`, `reporting_marts`, and `exports`
- emits lifecycle events to `observability_and_run_control`
- serves as the backend entry boundary for `operator_ui`

## Layer Boundary

- This module belongs to the backend/application layer.
- It may be called by the frontend, but it must not contain frontend widgets or screen logic.
- It may call DB services, but DB schema and SQL ownership remain in `storage_and_migrations`.

## Key Design Constraints

- keep orchestration separate from business logic
- support both scripted execution and future desktop UI
- avoid circular imports across services

## Main Deliverables

- top-level package layout under `src/moto_app/app/`
- application bootstrap module
- run coordinator facade
- app version metadata

## Locked Decisions

- This module is backend orchestration only and will be built before frontend work begins.
- The backend must support a headless execution mode before a UI is introduced.
- A single run coordinator should orchestrate configuration loading, migration checks, reference refresh actions, ingestion, transformation, marts, exports, and run-state updates.
- Frontend interactions should invoke backend workflows through this module rather than reaching directly into lower-level services.
- CLI compatibility should remain available during the migration so backend validation can happen before UI delivery.
- Phase-1 backend orchestration is implemented through `src/moto_app/app/service.py` and the headless entry script `database/tools/run_weekly.py`.
- The current headless coordinator returns stage timing and stage summary data so the future UI can show backend progress and post-run diagnostics without re-deriving them.

## Planned Orchestration Flow

Baseline weekly run flow:

1. load configuration
2. initialize logging and run-state services
3. validate and or apply DB migrations
4. create run record
5. validate source input and archive raw file
6. load stage data
7. execute transformation and silver persistence
8. build reporting marts
9. generate requested exports
10. finalize run status and output metadata

Planned backend package layout:

```text
src/
  moto_app/
    app/
```

## Remaining Open Decisions

- whether to use a lightweight service container or explicit object wiring
- whether CLI remains a supported secondary interface after GUI rollout

## Task Checklist

- [x] define application entrypoints for backend run execution
- [x] create a single run coordinator interface
- [x] wire configuration, DB access, logging, and service modules together
- [x] define how frontend actions invoke backend workflows
- [x] expose application version and runtime mode metadata
- [x] document startup and shutdown flow
