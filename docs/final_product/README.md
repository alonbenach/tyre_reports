# Final Product Plan

This folder defines the planned module structure for the SQLite-backed motorcycle reporting application. The goal is to break implementation into bounded work packages before code changes begin.

Long-form overview document:

- [`Moto_Weekly_Operator_App_Overview.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/Moto_Weekly_Operator_App_Overview.md)
- [`Development_Workflow.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/Development_Workflow.md)

## Target Product

The final product is a local Windows application that:

- ingests weekly CSV exports from `platformaopon.pl`
- stores processed state in SQLite
- generates Excel/PDF management reports
- guides a non-technical operator through the weekly run
- logs runs, errors, and outputs for auditability

## Planned Folder Structure

```text
docs/
  final_product/
    README.md
    modules/
      01_app_shell.md
      02_configuration.md
      03_storage_and_migrations.md
      04_reference_data.md
      05_ingestion.md
      06_transformation.md
      07_reporting_marts.md
      08_exports.md
      09_operator_ui.md
      10_observability_and_run_control.md
      11_packaging_and_distribution.md
      12_testing_and_parity.md
      13_access_control_and_admin_mode.md
      14_environments_and_cicd.md

src/
  moto_app/
    app/
    config/
    db/
    reference_data/
    ingest/
    transform/
    marts/
    exports/
    ui/
    observability/

database/
  migrations/
  tools/
tests/
```

## Architecture Boundaries

The planned product is divided into three explicit layers so the backend migration can be completed and validated before frontend work begins.

### Frontend Layer

Owns operator interaction only.

- desktop screens
- form inputs
- progress display
- output access
- instructions and troubleshooting content

Planned module:

- `operator_ui`

Target code area:

```text
src/
  moto_app/
    ui/
```

### Backend/Application Layer

Owns workflow orchestration and business logic.

- application bootstrap
- run orchestration
- ingestion services
- transformation services
- marts/report dataset builders
- export generation
- logging and run-state services

Planned modules:

- `app_shell`
- `configuration`
- `reference_data`
- `ingestion`
- `transformation`
- `reporting_marts`
- `exports`
- `observability_and_run_control`

Target code area:

```text
src/
  moto_app/
    app/
    config/
    reference_data/
    ingest/
    transform/
    marts/
    exports/
    observability/
```

### Data/DB Layer

Owns persistence and database structure.

- SQLite schema
- migrations
- connections
- transactions
- repositories/query services
- SQL views

Planned module:

- `storage_and_migrations`

Target code area:

```text
src/
  moto_app/
    db/

database/
  migrations/
  tools/
```

## Communication Rules

- The frontend layer must not own business logic.
- The frontend layer should call backend services, not talk directly to SQLite.
- The backend/application layer owns business rules and orchestrates every run.
- The data/DB layer owns persistence and schema concerns only.
- Report exports should read from SQL-backed services or query adapters, not from manually edited files.

## Module Map

1. `app_shell`
   Backend application entrypoints and dependency wiring.
2. `configuration`
   Backend configuration, paths, app constants, and runtime options.
3. `storage_and_migrations`
   Data/DB layer for SQLite schema, migration runner, connections, transactions, repositories.
4. `reference_data`
   Backend service for controlled import and use of mapping, price list, and campaign reference sources.
5. `ingestion`
   Backend service for source CSV validation, archive, metadata capture, and stage writes.
6. `transformation`
   Backend service for motorcycle filtering, canonical enrichment, normalization, and silver-layer logic.
7. `reporting_marts`
   Backend service for gold-layer marts and queryable report datasets.
8. `exports`
   Backend export service for Excel and PDF generation using SQL-backed data.
9. `operator_ui`
   Frontend desktop dashboard and guided operator workflow.
10. `observability_and_run_control`
    Backend observability services for logging, run registry, status tracking, retries, and operator-safe errors.
11. `packaging_and_distribution`
    Build, install, executable packaging, and deployment rules.
12. `testing_and_parity`
    Validation against the current pipeline and regression protection.
13. `access_control_and_admin_mode`
    Single-user lock model, read-only secondary access, and admin-only capabilities.
14. `environments_and_cicd`
    Development vs production environment model, branch policy, and CI/CD/test gates.

## Suggested Delivery Sequence

### Phase 1: Data/DB Foundation

1. Storage and migrations
2. Configuration

### Phase 2: Backend Migration

3. Reference data
4. Ingestion
5. Transformation
6. Reporting marts
7. Observability and run control
8. Exports
9. Testing and parity

### Phase 3: Frontend

10. Operator UI

### Phase 4: Release

11. Packaging and distribution
12. Access control and admin mode
13. Environments and CI/CD

Current planning direction for phase-4 work:

- one repository and one codebase
- explicit `dev` and `prod` environments
- future work branched from `main`
- CI-enforced tests before merges to `main`
- packaged production runtime distributed from a shared-drive location

## Task Control Guidance

Each module file under [`docs/final_product/modules`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules) includes a `Task Checklist` section. These checklists are intended to act as implementation tickets.

Recommended usage:

- mark checklist items complete as tasks are implemented
- keep architecture decisions in the module spec where they affect scope
- do not start frontend checklist items until backend parity is acceptable
- use `testing_and_parity` as a gate between backend migration and frontend work

## Notes

- Scope is motorcycle data only.
- SQLite is the system of record for processed data in phase 1.
- Raw CSV snapshots remain archived on disk.
- Excel files may remain source material for reference-data refreshes, but not as live runtime dependencies.
- Excel and PDF remain output formats, not storage layers.
- Frontend work should start only after backend parity is acceptable.
- Packaged operation is planned as a shared-drive app with runtime components kept close together for operator convenience.
- Only one user should be allowed to hold the writable/operator session at a time; additional users should open in read-only mode.
