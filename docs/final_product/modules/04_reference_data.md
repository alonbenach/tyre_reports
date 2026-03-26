# Module Spec: Reference Data

## Purpose

Control ingestion, versioning, and runtime use of canonical mappings, campaign rules, and price lists.

This module is part of the backend/application layer.

## Responsibilities

- import reference spreadsheets into SQLite-managed tables
- validate reference schema and freshness
- expose business-ready lookup datasets to transformation logic
- record provenance for each reference refresh

## Functional Scope

- canonical fitment mapping import
- campaign rule import
- price list import
- reference version metadata
- optional admin refresh workflow

## Inputs

- controlled Excel or CSV source files from `data/campaign rules/`
- refresh requests from CLI or UI

## Outputs

- normalized `ref_*` tables in SQLite
- reference refresh logs
- validated lookup datasets for transformation and marts logic

## Communication With Other Modules

- reads config and DB access from `configuration` and `storage_and_migrations`
- serves lookup data to `transformation` and `reporting_marts`
- emits refresh results to `observability_and_run_control`
- may be triggered from `operator_ui` admin actions

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns reference-data workflows and validation, not direct UI behavior.
- It may persist to SQLite through the data layer, but it does not own schema infrastructure.

## Key Design Constraints

- runtime logic must read from SQL, not directly from spreadsheets
- reference refreshes must be traceable
- reference import failures must be operator-readable

## Main Deliverables

- reference import services
- validation rules
- provenance metadata model
- admin refresh workflow

## Locked Decisions

- Runtime business logic will read only SQL-backed reference tables.
- The source authoring format remains spreadsheet-based in phase 1.
- Reference refresh should be an explicit backend or admin action, not an automatic step in every weekly run.
- Weekly pipeline runs are manually triggered in phase 1.
- The initial reference source files are:
  - `data/campaign rules/campaign 2026.xlsx`
  - `data/campaign rules/canonical fitment mapping.xlsx`
  - `data/campaign rules/price list Pirelli and competitors.xlsx`
- Each reference refresh must record source file path, checksum, refresh timestamp, and outcome status.

## Initial Reference Domains

Planned SQL-managed domains:

- canonical fitment mapping
- campaign rules
- price list and discount context
- refresh metadata

## Planned Tables

- `reference_refresh_runs`
- `ref_canonical_fitment_mapping`
- `ref_campaign_customer_discounts`
- `ref_campaign_pattern_extras`
- `ref_price_list`

Optional later split if needed:

- `ref_price_list_items`
- `ref_brand_aliases`

## Provenance Model

Every explicit reference refresh should capture:

- refresh run id
- source file path
- file checksum
- imported at timestamp
- refresh status
- error message if refresh failed

## Runtime Consumption Contract

- `transformation` reads canonical mappings and price/campaign context from SQL tables
- `reporting_marts` may read reference tables only where business logic requires it
- `operator_ui` should trigger refresh indirectly through backend services

## Remaining Open Decisions

- fallback behavior when a reference refresh fails but prior SQL-backed references exist

## Future Admin Reference Maintenance

Phase 1 keeps reference authoring outside the app so campaign mappings and price lists can still be curated manually with extra care.

Future versions should support an admin-maintained reference workflow inside the product for cases such as:

- yearly motorcycle campaign rollover
- new campaign discount rules
- new yearly competitor price lists
- IP-code mapping changes caused by material switches or product discontinuations

That future workflow should let an admin:

- load new source files for a new campaign year
- validate the expected columns and sheet structure before replacing active references
- review what reference domains are being changed
- apply the refresh with clear provenance and rollback visibility

This should stay an admin-only workflow and should not be mixed into the normal weekly operator path.

## Task Checklist

- [x] inventory current reference spreadsheets and their required columns
- [x] define normalized `ref_*` tables in SQLite
- [x] define provenance metadata for each refresh
- [x] implement spreadsheet-to-SQL import flow
- [ ] implement validation and operator-safe error messages for reference refreshes
- [x] define how backend services consume reference datasets at runtime
- [x] decide whether refresh is automatic or operator-triggered
- [x] define sheet-level mapping from each workbook to target SQL tables
- [x] define refresh granularity: all references in one transaction vs per-source transaction
- [ ] define fallback behavior when a reference refresh fails but prior SQL-backed references exist
- [ ] design an admin-managed yearly reference update workflow for campaign rules, price lists, and IP-code mappings
