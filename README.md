# Moto Weekly Report Pipeline

Weekly motorcycle reporting application for Platforma Opon producer stats, focused on Pirelli vs top competitors.

This repository now contains two things at once:

- the legacy file-based pipeline under `scripts/`
- the current SQLite-backed application under `src/moto_app/` and `database/tools/`

For product planning and module checkpoints, use:

- [`docs/final_product/README.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/README.md)

## What It Builds

- `data/raw/snapshot_date=YYYY-MM-DD/source.csv` archived raw files
- `data/silver/motorcycle_weekly.parquet` (or CSV fallback)
- `data/gold/*.csv` marts for market, brands, fitments, sellers, positioning, recap, and mapping quality
- `reports/PRICE_POSITIONING_Wxx_Poland.xlsx`
- `reports/PRICE_POSITIONING_Wxx_Poland.pdf` (if `matplotlib` is installed)
- `reports/offeror_focus_Wxx_Poland.xlsx`
- `reports/offeror_focus_Wxx_Poland.pdf` (if `matplotlib` is installed)

## Scope Implemented

- Motorcycle only (`type == "Motocykle"`)
- Recap brands (page 1): `Pirelli`, `Metzeler`, `Michelin`, `Continental`, `Bridgestone`, `Dunlop`
- Focus brands (rest of report): `Pirelli`, `Michelin`, `Bridgestone`, `Dunlop`, `Continental`
- Canonical layer using:
  - `data/campaign rules/canonical fitment mapping.xlsx`
  - `data/campaign rules/price list Pirelli and competitors.xlsx`
  - `data/campaign rules/campaign 2026.xlsx`
- Campaign enrichment:
  - Opon All-In discount
  - +3% extra discount pattern-set list
- Canonical matching:
  - brand + size-root candidate generation
  - exact/fuzzy pattern matching with confidence labels
- Price positioning and recap:
  - vs top competitors (median price gap)
  - week-over-week deltas using previous available ISO week snapshot
  - year-over-year deltas using same ISO week when available
- Segment cut by `key fitments` / `size-root` (rim grouping deprecated for analysis)
- Fitment proxy via product name (`FRONT`/`REAR`)
- Seller comparison by focus brands
- Page 1 recap logic:
  - built on canonical segment reference groups (10 groups)
  - Pirelli-weighted baseline index (`Pirelli = 100`)
  - missing segment handling by mean imputation (not zero)
- Multi-page PDF with:
  - Italy-style page-1 recap matrix
  - competitor trends + footprint
  - price-positioning heatmaps by rim group
  - fitment/segment dynamics
  - seller bubble map and checkpoint table
  - key-fitment checkpoint table (top stocked fitments by brand)

## Preferred Runtime Entry Points

Current application entry points:

```powershell
.\.venv\Scripts\python.exe database/tools/launch_ui.py
```

```powershell
.\.venv\Scripts\python.exe database/tools/run_weekly.py
```

Explicit environment launchers:

```powershell
.\.venv\Scripts\python.exe database/tools/run_app_dev.py
.\.venv\Scripts\python.exe database/tools/run_app_prod.py
```

Database utilities:

```powershell
.\.venv\Scripts\python.exe database/tools/init_db.py
.\.venv\Scripts\python.exe database/tools/load_reference_data.py
.\.venv\Scripts\python.exe database/tools/check_parity.py
```

## Legacy Script Flow

The original file-based scripts are still present for reference:

```powershell
python scripts/run_weekly.py
```

The default run now rebuilds the data pipeline once and generates both reports in sequence.

Excel only:

```powershell
python scripts/run_weekly.py --skip-pdf
```

Step-by-step:

```powershell
python scripts/ingest_weekly.py
python scripts/transform_motorcycle.py
python scripts/build_marts.py
python scripts/generate_report.py --skip-pdf
python scripts/generate_offeror_focus.py --skip-pdf
```

## Notes

- Input weekly files are expected at `data/ingest/YYYY-MM-DD.csv`.
- The current app also stages intake files through the GUI before running them.
- CSV parser uses tolerant decoding (`encoding_errors="replace"`) to handle mixed source text encodings.
- For very large histories, consider incremental processing and a database-backed gold layer.
- To show logos in charts, place files in `assets/logos/` (see `assets/logos/README.md`).
- The current SQLite-backed app stores runtime state under `database/`, `logs/`, and `reports/`.
- New QA tables:
  - `data/gold/gold_mapping_match_quality_weekly.csv`
  - `data/gold/gold_keyfitment_checkpoint_weekly.csv`
  - `data/gold/gold_recap_by_brand_weekly.csv`
  - `data/gold/gold_recap_by_brand_latest.csv`
- Commit hygiene:
  - `.gitignore` excludes source data, campaign files, generated reports, and operator runtime artifacts.
  - Test fixtures and committed snapshots under `tests/` are intentionally versioned as part of CI.

## Quality Gates

Local development checks:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest
```

To intentionally refresh committed test snapshots after a reviewed output change:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_snapshot_pipeline.py --update-snapshots
```

CI runs the same lint + test gates through GitHub Actions before changes should be merged into `main`.
