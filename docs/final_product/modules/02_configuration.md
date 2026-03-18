# Module Spec: Configuration

## Purpose

Centralize application settings, paths, constants, and runtime defaults so the app behaves predictably across environments.

This module is part of the backend/application layer.

## Responsibilities

- define app paths for data, intake, database, logs, reports, and assets
- validate required directories on startup
- store operator-safe defaults
- expose report mode and run-mode options

## Functional Scope

- path resolution
- configuration loading
- default values
- configuration validation

## Inputs

- application root path
- optional local config overrides
- runtime options from CLI or UI

## Outputs

- typed configuration object
- validated directory paths
- runtime feature flags

## Communication With Other Modules

- consumed by all other modules
- especially important for `storage_and_migrations`, `reference_data`, `exports`, and `operator_ui`

## Layer Boundary

- This module belongs to the backend/application layer.
- It provides runtime settings and path resolution to other layers.
- It should not contain UI widgets or SQL business logic.

## Key Design Constraints

- configuration must be readable by non-technical operators if exposed
- path layout should support packaging into a Windows desktop app
- database location should be stable and predictable

## Main Deliverables

- config model
- path helper utilities
- startup validation
- user-facing description of where files live

## Locked Decisions

- The backend will use a typed application settings object, exposed as a single config model at startup.
- Mutable operator settings will be stored in a local JSON file rather than in SQLite for phase 1.
- The packaged app will treat the project data area as the working-data root in development and will resolve a user-data location in packaged mode.
- The SQLite database path for development planning is `database/moto_pipeline.db`.
- Reports remain under `reports/`.
- Operator-selected weekly CSV files may be staged under `data/ingest/` before a run.
- Raw snapshot archives remain under `data/raw/`.
- Log files should live under a dedicated logs directory, planned as `logs/`.
- The configuration layer will expose both backend-facing settings and a reduced set of frontend-safe display paths.
- Phase-1 configuration is implemented in `src/moto_app/config/service.py` with a typed `AppConfig`, development runtime detection, directory bootstrap, and optional JSON override loading.

## Planned Path Layout

Development-oriented layout:

```text
data/
  ingest/
  raw/
  campaign rules/
database/
  moto_pipeline.db
reports/
logs/
assets/
```

Planned backend package layout:

```text
src/
  moto_app/
    config/
```

## Runtime Settings Contract

The configuration object should expose at least:

- application root path
- data directory
- intake directory
- raw archive directory
- database path
- reports directory
- logs directory
- assets directory
- reference source directory
- default report mode
- default PDF behavior
- development vs packaged runtime mode

## Remaining Open Decisions

- exact packaged user-data location on Windows
- whether logs should be retained by count, age, or both

## Task Checklist

- [x] define the configuration model and defaults
- [x] define all required filesystem paths
- [x] choose operator-config storage format if needed
- [x] implement path validation and directory bootstrap rules
- [x] document runtime settings exposed to backend and frontend
- [x] define environment detection for development vs packaged runtime
- [x] define path override policy for power users or support scenarios
- [ ] define how packaged app builds resolve user-data locations
