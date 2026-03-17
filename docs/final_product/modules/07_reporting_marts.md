# Module Spec: Reporting Marts

## Purpose

Build the gold-layer datasets that power the reports and future dashboard views.

This module is part of the backend/application layer.

## Responsibilities

- compute market, brand, fitment, seller, recap, and positioning marts
- expose stable datasets for both report generators
- provide latest-snapshot query views

## Functional Scope

- current mart logic migration from file outputs to SQL tables
- latest-period views
- query services for report generation
- quality tables and checkpoint datasets

## Inputs

- silver-layer records from `transformation`
- reference data as needed
- run context from `observability_and_run_control`

## Outputs

- `gold_*` tables
- latest-report SQL views
- mart-level quality metrics

## Communication With Other Modules

- called by `app_shell`
- reads and writes via `storage_and_migrations`
- supplies datasets to `exports`
- logs metrics and completion state to `observability_and_run_control`

## Layer Boundary

- This module belongs to the backend/application layer.
- It owns report dataset construction, not UI rendering or DB schema management.
- It should expose stable read models for exports and future frontend consumption.

## Key Design Constraints

- preserve report parity with the current output set
- keep marts queryable for future dashboard expansion
- avoid recomputing unaffected history when not necessary

## Main Deliverables

- gold dataset builders
- query views for latest report slices
- mart-level validation outputs

## Locked Decisions

- The first migration target is parity with the current gold outputs, not a redesign of report logic.
- Gold datasets should be persisted as SQL tables for the report-producing marts that are expensive or reused.
- Latest-period convenience reads should be exposed as SQL views.
- Report-specific latest views are preferred over one overly generic latest view.
- The marts layer should continue to produce the existing reporting families: market, brand, segment/fitment, seller, price positioning, mapping quality, key-fitment checkpoint, and recap.
- The first SQL-backed implementation will rebuild the full gold layer from the current silver dataset rather than refresh one snapshot in isolation.

## Current Gold Outputs To Preserve

The SQL-backed marts should preserve equivalents of:

- `gold_market_weekly`
- `gold_brand_weekly`
- `gold_segment_weekly`
- `gold_seller_weekly`
- `gold_fitment_weekly`
- `gold_price_positioning_weekly`
- `gold_mapping_match_quality_weekly`
- `gold_keyfitment_checkpoint_weekly`
- `gold_recap_by_brand_weekly`
- `gold_recap_by_brand_latest`

## Persistence Strategy

Persist as tables in phase 1:

- weekly gold datasets used directly by the reports
- quality and checkpoint datasets needed for diagnostics and report sections

Expose as views in phase 1:

- latest successful recap slice
- latest positioning output slice
- latest offeror-facing read models where helpful

Planned backend package layout:

```text
src/
  moto_app/
    marts/
```

## Remaining Open Decisions

- whether seller, fitment, and segment marts should remain separate physical tables long-term or later be derived from a shared persisted analytical base
- whether presentation-oriented recap display fields should be stored strictly as text display values or normalized for easier parity comparisons

## Task Checklist

- [x] inventory current gold outputs and map them to SQL datasets
- [x] define schemas for `gold_*` tables
- [x] choose which marts are persisted and which are views
- [x] implement latest-snapshot query views
- [x] define mart refresh scope for a rerun of one snapshot
- [x] define which mart outputs are required for report generation vs diagnostic-only
- [x] migrate current mart logic from file outputs to SQL-backed services
- [x] validate key metrics against current reports
- [x] document the dataset contract used by report exports
