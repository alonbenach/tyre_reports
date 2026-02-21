# Moto Weekly Report Pipeline

Weekly data pipeline for Oponeo producer stats, focused on motorcycle analysis for Pirelli vs top competitors.

## What It Builds

- `data/raw/snapshot_date=YYYY-MM-DD/source.csv` archived raw files
- `data/silver/motorcycle_weekly.parquet` (or CSV fallback)
- `data/gold/*.csv` marts for market, brands, fitments, sellers, positioning, recap, and mapping quality
- `reports/PRICE_POSITIONING_Wxx_Poland.xlsx`
- `reports/PRICE_POSITIONING_Wxx_Poland.pdf` (if `matplotlib` is installed)

## Scope Implemented

- Motorcycle only (`type == "Motocykle"`)
- Recap brands (page 1): `Pirelli`, `Metzeler`, `Michelin`, `Continental`, `Bridgestone`, `Dunlop`
- Focus brands (rest of report): `Pirelli`, `Michelin`, `Bridgestone`, `Dunlop`, `Continental`
- Canonical layer using:
  - `data/campaign rules/canonical fitment mapping.xlsx`
  - `data/campaign rules/price list Pirelli and competitors.xlsx`
  - `data/campaign rules/campaign 2026.xlsx`
- Campaign enrichment:
  - Oponeo All-In discount
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

## Run

```powershell
python scripts/run_weekly.py
```

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
```

## Notes

- Input weekly files are expected at `data/YYYY-MM-DD.csv`.
- CSV parser uses tolerant decoding (`encoding_errors="replace"`) to handle mixed source text encodings.
- For very large histories, consider incremental processing and a database-backed gold layer.
- To show logos in charts, place files in `assets/logos/` (see `assets/logos/README.md`).
- New QA tables:
  - `data/gold/gold_mapping_match_quality_weekly.csv`
  - `data/gold/gold_keyfitment_checkpoint_weekly.csv`
  - `data/gold/gold_recap_by_brand_weekly.csv`
  - `data/gold/gold_recap_by_brand_latest.csv`
- Commit hygiene:
  - `.gitignore` excludes all source data, campaign files, snapshots, generated reports, and instructions.
  - Only pipeline code/architecture and docs should be committed.
