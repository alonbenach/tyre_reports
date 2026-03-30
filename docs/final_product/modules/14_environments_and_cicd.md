# Module Spec: Environments and CI/CD

## Purpose

Define how development and production environments differ, how code moves safely toward release, and how automated checks enforce quality before changes reach `main`.

This module is cross-cutting and applies to engineering workflow rather than a single runtime layer.

## Responsibilities

- define development vs production environment behavior
- define branch and merge policy
- define CI checks and release gates
- document the packaging/release path for shared-drive deployment

## Functional Scope

- development environment setup
- production/shared-drive runtime model
- CI test execution
- merge protection
- release packaging workflow

## Inputs

- source code changes
- test suite results
- packaging scripts and version metadata
- branch and release policy

## Outputs

- environment policy
- CI/CD blueprint
- required merge checks
- release and rollback guidance

## Communication With Other Modules

- depends on `testing_and_parity` for enforced quality gates
- depends on `packaging_and_distribution` for release artifacts
- depends on `configuration` for dev/prod path behavior
- depends on `access_control_and_admin_mode` for shared-drive production operation

## Layer Boundary

- This module is cross-cutting and process-oriented.
- It should not contain business logic.
- It should define engineering and deployment rules that other modules must satisfy.

## Key Design Constraints

- development must continue safely after production rollout
- production behavior must be predictable on the shared drive
- automated tests must block unsafe merges to `main`
- CI/CD should stay understandable and maintainable for a small internal project

## Main Deliverables

- dev vs prod environment policy
- CI workflow blueprint
- merge gate policy
- release workflow and rollback notes
- developer branch / commit / push / correction workflow

## Locked Decisions

- Development and production environments are both required from this point onward.
- The production app will be distributed from a shared-drive location with runtime components kept close together for convenience.
- CI/CD should enforce automated tests before merges to `main`.
- The project needs a blueprint `.md` file before CI/CD implementation proceeds.
- There will be one repository and one codebase, not separate development and production apps.
- Development and production will differ by environment configuration, runtime paths, launch entrypoints, and release rules rather than by business logic.
- The app should have separate environment launchers, conceptually `run_app_dev` and `run_app_prod`, both targeting the same application code.
- Future work should branch from `main`.
- Changes should reach `main` through pull requests rather than direct feature development on `main`.

## Planned Environment Model

Development:

- local repo checkout
- editable source tree
- developer-oriented config overrides
- test DBs and temp artifacts separated from production runtime assets
- local dev launcher such as `run_app_dev`
- safe-to-break environment for experimentation and iterative changes

Production:

- packaged app launched from shared-drive location
- shared runtime area containing executable, DB, logs, reports, intake, and related assets close together
- restricted operational behavior with single-user writable session rules
- production launcher such as `run_app_prod`
- conservative runtime behavior intended for non-technical operators

## Environment Separation Rules

The planned split is:

- one repository
- one codebase
- one application logic path
- two runtime environments

What may differ between `dev` and `prod`:

- database file location
- intake folder
- raw archive location
- reports folder
- logs folder
- config override source
- safety restrictions and feature flags
- packaging/distribution behavior

What should not differ between `dev` and `prod`:

- transformation rules
- report calculation logic
- SQL schema intent
- report family behavior

This rule exists so development can continue safely without creating two drifting versions of the app.

## Launcher Strategy

Implemented phase-1 environment entrypoint model:

- `run_app_dev`
  - launches the app against development-oriented paths and settings
- `run_app_prod`
  - launches the app against production/shared-drive paths and settings

Current implementation files:

- `database/tools/run_app_dev.py`
- `database/tools/run_app_prod.py`

`launch_ui.py` remains as a development-oriented compatibility launcher.

The important point is that environment selection is now explicit and predictable.

Current interpretation:

- `dev` is the working development/runtime context used from the repository
- `prod` is the production-target runtime context used to prepare packaging and deployment behavior
- true isolation between development and production comes from packaging and distribution of a reviewed build, not from config split alone
- recent packaged-runtime debugging has reinforced that packaged smoke validation must remain part of the release discipline, because runtime path and persistence bugs can still exist even when repository tests are green

## Planned CI/CD Model

Proposed baseline:

- feature branches created from `main`
- pull requests targeting `main`
- CI runs on each pull request to `main`
- required checks before merge:
  - automated tests
  - environment/setup sanity checks
  - backend parity/regression checks where practical
  - packaging smoke checks later

## Branch and Merge Policy

Recommended working policy:

- `main` is the stable integration branch
- new work starts from short-lived feature branches
- pull requests are required for changes into `main`
- direct commits to `main` should be avoided once CI enforcement is active
- merge should only be allowed when required checks pass

Suggested branch naming examples:

- `feature/access-control`
- `feature/admin-mode`
- `feature/cicd-foundation`
- `fix/output-browser`

## Minimum CI Gate for Phase 1

The first useful CI gate should be intentionally small and reliable.

Implemented initial required checks:

1. install dependencies successfully
2. run `ruff` as the lint gate
3. run `pytest`
4. fail the push / pull request if either check fails

Current implementation choices:

- CI provider: GitHub Actions
- workflow file: `.github/workflows/ci.yml`
- runtime dependencies stay in `requirements.txt`
- development-only tooling is installed from `requirements-dev.txt`
- snapshot baselines are committed and updated only through the local `--update-snapshots` workflow
- developer workflow guide lives in `docs/final_product/Development_Workflow.md`

This initial gate is more valuable than a larger but unreliable CI design.

## Planned CI Expansion After Phase 1

After the basic test gate is working, CI can expand to include:

- parity checks for key backend/report behaviors
- packaging smoke tests
- possibly lint/format checks if you decide they add value
- release artifact creation on tagged versions or release branches

Release path:

- merge to `main`
- create packaged build
- validate on staging/test runtime location
- promote build to shared-drive production location

## Packaging and Promotion Model

Recommended promotion sequence:

1. develop and test on `dev`
2. merge approved changes into `main`
3. build a production package from `main`
4. validate that package in a non-production runtime location
5. copy/promote the approved package into the shared-drive production location

This keeps production tied to a known reviewed state rather than an active development folder.

Locked packaging direction:

- build a portable Windows application folder first
- use `PyInstaller` as the preferred initial packaging tool
- collapse the final operator-facing production package to one flat root folder instead of exposing `runtime/prod/`

## Rollback Direction

If a packaged build is bad, the preferred rollback model should be:

- keep the prior good production package/version available
- restore the prior package
- preserve production DB and logs unless the issue requires explicit data rollback
- document what version was rolled back and why

## Remaining Open Decisions

- whether packaging is built on every merge or only on tagged releases
- whether production uses a dedicated staging shared folder before promotion

## Task Checklist

- [x] define dev vs prod runtime path behavior in detail
- [x] define config override policy for development without risking production runtime data
- [x] define branch, PR, and merge policy
- [x] define mandatory automated test gates before merge to `main`
- [x] define packaging/release workflow from `main` to shared-drive deployment
- [x] define rollback procedure for a bad production package
- [x] write CI workflow blueprint for future implementation
- [x] choose CI provider and initial workflow file layout
- [x] define dependency-group strategy for runtime vs development tooling
- [x] document the developer branch / commit / push / correction cycle
- [x] define exact dev and prod launcher/entrypoint files
