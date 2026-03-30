# Module Spec: Packaging and Distribution

## Purpose

Define how the application is built, packaged, installed, and updated for non-technical Windows users.

This module is cross-cutting and applies after frontend, backend, and data layers are stable.

## Responsibilities

- package the application into an operator-friendly form
- define installation prerequisites
- standardize folder locations and update process
- document deployment steps

## Functional Scope

- Windows executable packaging
- dependency bundling
- install/update process
- file-location strategy
- operator handoff material

## Inputs

- finalized application code
- packaging configuration
- release version metadata

## Outputs

- installable or portable application package
- packaging instructions
- release checklist

## Communication With Other Modules

- depends on stable paths from `configuration`
- packages `operator_ui`, `app_shell`, and runtime services
- informs where logs, DB, assets, and reports are stored

## Layer Boundary

- This module is not part of a single runtime layer.
- It packages frontend, backend, and data-layer artifacts into a distributable product.
- It should not introduce business logic changes.

## Key Design Constraints

- installation should not require developer tooling
- operator should not need to activate a virtual environment
- app data and executable layout must be predictable

## Main Deliverables

- packaging config
- release process
- install/update guide
- operator handoff checklist

## Phase-1 Packaging Foundation

Current implementation foundation:

- `packaging/MotoWeeklyOperator.spec`
- `packaging/build_portable.ps1`
- explicit production launcher target: `database/tools/run_app_prod.py`

Current scope of that foundation:

- builds toward a `PyInstaller` portable folder package
- packages the production launcher instead of the development launcher
- creates the expected operator-facing runtime folder skeleton after build
- writes placeholder readme files into runtime folders that still require manual reference-file maintenance

## Open Decisions

- update mechanism for new application versions

## Locked Direction

- Production packaging should align with the planned `prod` environment rather than reusing the live development runtime directly.
- The shared-drive production package should keep related runtime components close together for operator convenience.
- Packaging should eventually align with explicit environment launchers such as `run_app_dev` and `run_app_prod`.
- The current pre-packaging implementation models production runtime paths under `runtime/prod/` so the future shared-drive folder layout can be exercised before packaging is finalized.
- The first packaging target should be a portable Windows folder, not an MSI-style installer.
- The preferred first packaging tool is `PyInstaller`.
- The final operator-facing production package should use a flat root folder layout rather than exposing `runtime/prod/` to users.
- Operators should receive a packaged app folder containing the executable plus nearby runtime folders such as:
  - `database/`
  - `data/campaign rules/`
  - `data/ingest/`
  - `data/raw/`
  - `reports/`
  - `logs/`
  - `assets/`

## Planned Production Folder Layout

Target operator-facing production layout:

```text
MotoWeeklyOperator/
  MotoWeeklyOperator.exe
  database/
  data/
    campaign rules/
    ingest/
    raw/
  reports/
  logs/
  assets/
```

The current `runtime/prod/` structure is a development and pre-packaging modeling aid only. It should not be the final operator-facing folder shape unless packaging work proves there is a strong reason to keep it.

## Phase-1 Packaging Foundation

Current implementation foundation:

- `packaging/MotoWeeklyOperator.spec`
- `packaging/build_portable.ps1`
- explicit production launcher target: `database/tools/run_app_prod.py`

Current scope of that foundation:

- builds toward a `PyInstaller` portable folder package
- packages the production launcher instead of the development launcher
- creates the expected operator-facing runtime folder skeleton after build
- writes placeholder readme files into runtime folders that still require manual reference-file maintenance

## Task Checklist

- [x] choose packaging format for Windows
- [x] define dependency bundling strategy
- [x] define installation layout for executable, DB, logs, and reports
- [x] create initial packaging config and build script scaffolding
- [ ] define upgrade/update procedure
- [ ] document operator installation and handoff steps
- [ ] create a release checklist for packaged builds
