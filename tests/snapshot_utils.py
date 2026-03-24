from __future__ import annotations

from pathlib import Path

import pandas as pd


SNAPSHOT_ROOT = Path(__file__).resolve().parent / "snapshots"


def _normalize_snapshot_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        series = normalized[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            normalized[column] = pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
            continue
        if pd.api.types.is_bool_dtype(series):
            normalized[column] = series.fillna(False).astype(int)
            continue
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().any() and series.astype("string").str.match(r"^-?\d+(\.\d+)?$", na=False).any():
            normalized[column] = numeric.round(6)
            continue
        normalized[column] = series.fillna("").astype("string")

    return normalized


def assert_frame_matches_snapshot(
    *,
    name: str,
    df: pd.DataFrame,
    update_snapshots: bool,
) -> None:
    snapshot_path = SNAPSHOT_ROOT / name
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    normalized = _normalize_snapshot_frame(df)
    csv_text = normalized.to_csv(index=False, lineterminator="\n", float_format="%.6f")

    if update_snapshots or not snapshot_path.exists():
        snapshot_path.write_text(csv_text, encoding="utf-8")
        return

    expected = snapshot_path.read_text(encoding="utf-8")
    assert expected == csv_text, (
        f"Snapshot mismatch for {name}. "
        "If the change is intentional, rerun pytest with --update-snapshots and review the new snapshot."
    )
