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

## Open Decisions

- single portable folder vs installer-based deployment
- update mechanism for new application versions

## Task Checklist

- [ ] choose packaging format for Windows
- [ ] define dependency bundling strategy
- [ ] define installation layout for executable, DB, logs, and reports
- [ ] define upgrade/update procedure
- [ ] document operator installation and handoff steps
- [ ] create a release checklist for packaged builds
