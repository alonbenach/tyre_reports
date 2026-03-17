from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from moto_app.db.runtime import connect_sqlite


@dataclass(frozen=True)
class ParityCheckResult:
    name: str
    passed: bool
    details: str


def _read_table(connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table_name}", connection)


def _normalize_numeric_frame(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        if column in key_cols:
            out[column] = out[column].astype("string")
            continue
        numeric = pd.to_numeric(out[column], errors="coerce")
        if numeric.notna().any():
            out[column] = numeric.round(6)
    return out.sort_values(key_cols).reset_index(drop=True)


def _frames_match(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    try:
        assert_frame_equal(left, right, check_dtype=False, check_exact=False)
        return True
    except AssertionError:
        return False


def _metric_frames_match(
    left: pd.DataFrame,
    right: pd.DataFrame,
    key_cols: list[str],
) -> bool:
    metric_cols = [column for column in left.columns if column not in key_cols]
    merged = left.merge(right, on=key_cols, how="outer", suffixes=("_left", "_right"))
    if merged.empty:
        return True
    for column in metric_cols:
        left_col = f"{column}_left"
        right_col = f"{column}_right"
        if left_col not in merged.columns or right_col not in merged.columns:
            return False
        left_numeric = pd.to_numeric(merged[left_col], errors="coerce")
        right_numeric = pd.to_numeric(merged[right_col], errors="coerce")
        if left_numeric.notna().any() or right_numeric.notna().any():
            left_values = left_numeric.fillna(-999999.0).astype(float).to_numpy()
            right_values = right_numeric.fillna(-999999.0).astype(float).to_numpy()
            if not np.allclose(left_values, right_values, equal_nan=True):
                return False
            continue
        left_text = merged[left_col].astype("string").fillna("<NA>")
        right_text = merged[right_col].astype("string").fillna("<NA>")
        if not left_text.equals(right_text):
            return False
    return True


def collect_parity_results(
    *,
    db_path: Path,
    legacy_gold_dir: Path,
    legacy_silver_path: Path,
) -> list[ParityCheckResult]:
    results: list[ParityCheckResult] = []
    with connect_sqlite(db_path) as connection:
        gold_tables = {
            "gold_market_weekly": "gold_market_weekly.csv",
            "gold_brand_weekly": "gold_brand_weekly.csv",
            "gold_segment_weekly": "gold_segment_weekly.csv",
            "gold_seller_weekly": "gold_seller_weekly.csv",
            "gold_fitment_weekly": "gold_fitment_weekly.csv",
            "gold_price_positioning_weekly": "gold_price_positioning_weekly.csv",
            "gold_mapping_match_quality_weekly": "gold_mapping_match_quality_weekly.csv",
            "gold_keyfitment_checkpoint_weekly": "gold_keyfitment_checkpoint_weekly.csv",
            "gold_recap_by_brand_weekly": "gold_recap_by_brand_weekly.csv",
            "gold_recap_by_brand_latest": "gold_recap_by_brand_latest.csv",
        }
        for table_name, file_name in gold_tables.items():
            db_count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            legacy_count = len(pd.read_csv(legacy_gold_dir / file_name))
            results.append(
                ParityCheckResult(
                    name=f"{table_name}_row_count",
                    passed=db_count == legacy_count,
                    details=f"db={db_count}, legacy={legacy_count}",
                )
            )

        silver_db = _read_table(connection, "silver_motorcycle_weekly")
        silver_legacy = pd.read_parquet(legacy_silver_path)
        latest_snapshot = pd.to_datetime(silver_db["snapshot_date"], errors="coerce").max()
        silver_db_latest = silver_db[
            pd.to_datetime(silver_db["snapshot_date"], errors="coerce").eq(latest_snapshot)
        ].copy()
        silver_legacy_latest = silver_legacy[
            pd.to_datetime(silver_legacy["snapshot_date"], errors="coerce").eq(latest_snapshot)
        ].copy()
        results.append(
            ParityCheckResult(
                name="silver_latest_snapshot_row_count",
                passed=len(silver_db_latest) == len(silver_legacy_latest),
                details=f"db={len(silver_db_latest)}, legacy={len(silver_legacy_latest)}, snapshot={latest_snapshot.date()}",
            )
        )

        brand_keys = ["snapshot_date", "brand"]
        brand_metric_cols = brand_keys + ["rows", "unique_products", "unique_sellers", "stock_qty"]
        brand_db = _normalize_numeric_frame(
            _read_table(connection, "gold_brand_weekly")[brand_metric_cols],
            brand_keys,
        )
        brand_legacy = _normalize_numeric_frame(
            pd.read_csv(legacy_gold_dir / "gold_brand_weekly.csv")[brand_metric_cols],
            brand_keys,
        )
        brand_match = _metric_frames_match(brand_db, brand_legacy, brand_keys)
        results.append(
            ParityCheckResult(
                name="gold_brand_weekly_core_metrics",
                passed=brand_match,
                details="core brand metrics matched" if brand_match else "core brand metrics differ",
            )
        )

        positioning_keys = ["snapshot_date", "granularity", "analysis_fitment_key"]
        positioning_cols = positioning_keys + [
            "pirelli_median_price",
            "competitor_median_price",
            "market_median_price",
            "price_gap_vs_comp",
            "pirelli_price_index",
        ]
        positioning_db = _normalize_numeric_frame(
            _read_table(connection, "gold_price_positioning_weekly")[positioning_cols],
            positioning_keys,
        )
        positioning_legacy = _normalize_numeric_frame(
            pd.read_csv(legacy_gold_dir / "gold_price_positioning_weekly.csv")[positioning_cols],
            positioning_keys,
        )
        positioning_match = _metric_frames_match(
            positioning_db,
            positioning_legacy,
            positioning_keys,
        )
        results.append(
            ParityCheckResult(
                name="gold_price_positioning_weekly_core_metrics",
                passed=positioning_match,
                details="core positioning metrics matched"
                if positioning_match
                else "core positioning metrics differ",
            )
        )

        recap_keys = ["snapshot_date", "brand"]
        recap_cols = recap_keys + [
            "positioning_index_round",
            "vs_prev_week_round",
            "vs_py_round",
        ]
        recap_db = _normalize_numeric_frame(
            _read_table(connection, "gold_recap_by_brand_weekly")[recap_cols],
            recap_keys,
        )
        recap_legacy = _normalize_numeric_frame(
            pd.read_csv(legacy_gold_dir / "gold_recap_by_brand_weekly.csv")[recap_cols],
            recap_keys,
        )
        recap_match = _metric_frames_match(recap_db, recap_legacy, recap_keys)
        results.append(
            ParityCheckResult(
                name="gold_recap_by_brand_weekly_core_metrics",
                passed=recap_match,
                details="core recap metrics matched" if recap_match else "core recap metrics differ",
            )
        )

    return results


def assert_parity(
    *,
    db_path: Path,
    legacy_gold_dir: Path,
    legacy_silver_path: Path,
) -> list[ParityCheckResult]:
    results = collect_parity_results(
        db_path=db_path,
        legacy_gold_dir=legacy_gold_dir,
        legacy_silver_path=legacy_silver_path,
    )
    failed = [result for result in results if not result.passed]
    if failed:
        details = "; ".join(f"{result.name}: {result.details}" for result in failed)
        raise AssertionError(details)
    return results
