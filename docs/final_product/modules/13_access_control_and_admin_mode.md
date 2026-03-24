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

## Phase-1 Implementation Notes

Current implementation slice:

- backend lock service in `src/moto_app/access_control/service.py`
- shared-runtime JSON lock file with:
  - session id
  - user name
  - machine name
  - acquired timestamp
  - last heartbeat timestamp
  - session mode
- UI startup evaluation and writable/read-only banner in `src/moto_app/ui/app.py`
- writable-session heartbeat refresh while the app remains open
- automatic lock release on window close when the current session owns the lock
- service-level regression tests in `tests/test_access_control.py`

Current gap:

- admin activation UI and explicit stale-lock clear controls are not implemented yet
- the current UI only distinguishes writable vs read-only behavior

## Locked Decisions

- The packaged app should follow a first-come-first-serve operator model.
- Only one active writable session may exist at a time.
- Additional users may open the app in read-only mode only.
- Read-only mode should still allow status viewing, instructions, and output access.
- Write-capable actions such as staging, running, reference refresh, and admin edits must be disabled in read-only mode.
- Admin mode is required for ongoing development and support after initial rollout.
- Phase-1 lock implementation should use a shared-runtime JSON lock file rather than relying on SQLite write contention alone.
- The lock file should include a heartbeat timestamp so stale sessions can be detected without guessing from process state.
- Phase-1 admin entry should be controlled by a configured Windows-user allowlist, with development mode allowed to enable broader local testing if needed.
- Replace-snapshot should remain a normal operator action in phase 1, but only with the existing explicit confirmation and duplicate-snapshot safeguards.

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
- view lock ownership details and clear stale lock only after explicit confirmation

## Shared-Drive Lock Model

Recommended phase-1 model:

- store a JSON lock artifact in the shared runtime area near the DB and logs
- writable session acquires the lock during startup and refreshes a heartbeat while the app stays open
- lock captures:
  - Windows user name
  - machine name
  - acquired timestamp
  - last heartbeat timestamp
  - app version
  - session mode
- if lock exists and heartbeat is fresh:
  - app opens in read-only mode
  - UI explains who holds the writable session if metadata is available
- if lock appears stale:
  - normal users still open read-only
  - admin mode may clear the lock after explicit confirmation

## Recommended Phase-1 Timeouts

- heartbeat interval: `15` seconds
- stale-lock threshold: `5` minutes

These values are conservative enough for a desktop app and simple to reason about during support.

## Read-Only UI Requirements

When the app opens in read-only mode, it should:

- show a clear read-only banner at the top of the window
- identify the current writable-session owner when available
- allow navigation to:
  - home/status
  - outputs
  - instructions
  - run history
- disable:
  - CSV staging
  - run execution
  - reference refresh
  - admin actions
- explain why actions are disabled instead of failing after click

## Admin Entry Model

Recommended phase-1 model:

- config contains an `admin_users` allowlist of Windows usernames
- if current user is on the allowlist:
  - admin controls may be shown after explicit admin activation in the UI
- if current user is not on the allowlist:
  - app never exposes destructive or recovery actions

Development mode may later allow easier local admin testing, but production/shared-drive behavior should still be based on the allowlist.

## Support and Recovery Procedure

When a user reports that the app is read-only unexpectedly:

1. read the lock-owner metadata shown by the app
2. confirm whether another operator session is legitimately active
3. if the session is active, do not clear the lock
4. if the session is stale, use admin mode to clear the lock explicitly
5. record the recovery action in logs or run history if the implementation supports it

## Remaining Open Decisions

- exact lock-file location inside the runtime layout
- whether lock heartbeat should be stored only in file contents or also reflected in file modified time
- whether stale-lock clear actions should create a dedicated audit record beyond normal logs

## Task Checklist

- [x] choose the lock implementation strategy for shared-drive usage
- [x] define lock metadata fields and stale-lock timeout rules
- [x] define read-only UI behavior for secondary app instances
- [x] define the admin capability matrix
- [x] define how admin mode is entered and audited
- [x] define stale-lock recovery procedure
- [ ] add operator/support instructions for lock conflicts
- [x] implement the backend lock service
- [x] wire read-only fallback into the UI
- [ ] implement explicit admin activation and stale-lock recovery controls
- [x] add automated tests for lock acquisition, read-only fallback, and stale-lock recovery
