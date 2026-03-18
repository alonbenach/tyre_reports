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

## Locked Decisions

- Development and production environments are both required from this point onward.
- The production app will be distributed from a shared-drive location with runtime components kept close together for convenience.
- CI/CD should enforce automated tests before merges to `main`.
- The project needs a blueprint `.md` file before CI/CD implementation proceeds.

## Planned Environment Model

Development:

- local repo checkout
- editable source tree
- developer-oriented config overrides
- test DBs and temp artifacts separated from production runtime assets

Production:

- packaged app launched from shared-drive location
- shared runtime area containing executable, DB, logs, reports, intake, and related assets close together
- restricted operational behavior with single-user writable session rules

## Planned CI/CD Model

Proposed baseline:

- feature branches
- pull requests into `main`
- CI runs on each pull request
- required checks before merge:
  - automated tests
  - backend parity/regression checks where practical
  - packaging smoke checks later

Release path:

- merge to `main`
- create packaged build
- validate on staging/test runtime location
- promote build to shared-drive production location

## Remaining Open Decisions

- exact CI provider and workflow file location
- whether packaging is built on every merge or only on tagged releases
- whether production uses a dedicated staging shared folder before promotion

## Task Checklist

- [ ] define dev vs prod runtime path behavior in detail
- [ ] define config override policy for development without risking production runtime data
- [ ] define branch, PR, and merge policy
- [ ] define mandatory automated test gates before merge to `main`
- [ ] define packaging/release workflow from `main` to shared-drive deployment
- [ ] define rollback procedure for a bad production package
- [ ] write CI workflow blueprint for future implementation
