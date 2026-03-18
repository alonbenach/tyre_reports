# Module Spec: Access Control and Admin Mode

## Purpose

Define how the shared-drive application enforces a single active operator session, supports read-only secondary viewers, and exposes higher-risk administrative actions safely.

This module is cross-cutting and applies to frontend, backend/application, and packaging behavior.

## Responsibilities

- enforce one writable operator session at a time
- allow secondary app instances in read-only mode
- define admin-only capabilities and safeguards
- document stale-lock recovery and support handoff

## Functional Scope

- single-user operator lock
- read-only fallback mode
- admin mode entry and restrictions
- stale session recovery
- operator vs admin capability matrix

## Inputs

- application startup events
- current user and machine identity
- active lock metadata
- admin-mode requests

## Outputs

- writable or read-only UI state
- lock metadata for active session ownership
- admin-mode capability gating
- support diagnostics for lock conflicts

## Communication With Other Modules

- relies on `configuration` for lock-file or lease location
- surfaces mode state in `operator_ui`
- coordinates with `app_shell` at startup
- logs lock and admin actions through `observability_and_run_control`
- informs `packaging_and_distribution` shared-drive deployment rules

## Layer Boundary

- This module is cross-cutting.
- The backend should own lock acquisition and admin authorization logic.
- The UI should only reflect the resulting mode and disable actions accordingly.

## Key Design Constraints

- only one user may hold a writable/operator session at a time
- secondary users should still be able to inspect outputs and status
- admin actions must be clearly separated from routine operator actions
- recovery from stale locks must be explicit and supportable

## Main Deliverables

- startup lock service
- read-only UI mode behavior
- admin mode specification
- stale-lock recovery procedure

## Locked Decisions

- The packaged app should follow a first-come-first-serve operator model.
- Only one active writable session may exist at a time.
- Additional users may open the app in read-only mode only.
- Read-only mode should still allow status viewing, instructions, and output access.
- Write-capable actions such as staging, running, reference refresh, and admin edits must be disabled in read-only mode.
- Admin mode is required for ongoing development and support after initial rollout.

## Planned Capability Split

Operator mode:

- stage weekly CSV into intake
- run a staged snapshot
- replace an existing snapshot intentionally
- open outputs
- read instructions and status

Read-only mode:

- view run status
- view DB coverage
- open outputs
- read instructions

Admin mode:

- refresh reference data
- inspect detailed run history and logs
- remove mistaken staged intake files
- rebuild or replace selected snapshots intentionally
- recover from stale session locks

## Shared-Drive Lock Model

Recommended phase-1 model:

- store a lock artifact in the shared runtime area near the DB and logs
- lock captures:
  - user name
  - machine name
  - acquired timestamp
  - app version
- if lock exists and is healthy:
  - app opens in read-only mode
- if lock is stale:
  - only admin flow may clear it after explicit confirmation

## Remaining Open Decisions

- exact lock implementation: lock file vs SQLite-backed lease vs hybrid
- how admin mode is entered: local secret, config flag, Windows user allowlist, or similar
- whether replace-snapshot remains normal operator capability or becomes admin-only later

## Task Checklist

- [ ] choose the lock implementation strategy for shared-drive usage
- [ ] define lock metadata fields and stale-lock timeout rules
- [ ] define read-only UI behavior for secondary app instances
- [ ] define the admin capability matrix
- [ ] define how admin mode is entered and audited
- [ ] define stale-lock recovery procedure
- [ ] add operator/support instructions for lock conflicts

