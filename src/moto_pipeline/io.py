from __future__ import annotations

import hashlib
import logging
import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd


def ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def parse_snapshot_date(file_path: Path) -> str:
    # Expected input filename format: YYYY-MM-DD.csv
    return file_path.stem


def file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_to_raw_snapshot(src_file: Path, raw_dir: Path) -> Path:
    snapshot = parse_snapshot_date(src_file)
    target_dir = raw_dir / f"snapshot_date={snapshot}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "source.csv"
    shutil.copy2(src_file, target_file)
    return target_file


def read_weekly_csv(file_path: Path, usecols: list[str] | None = None) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        sep=";",
        usecols=usecols,
        dtype="string",
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )


def write_df(df: pd.DataFrame, output_path: Path, logger: logging.Logger) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == ".parquet":
        try:
            df.to_parquet(output_path, index=False)
            return output_path
        except Exception as exc:  # pragma: no cover
            csv_fallback = output_path.with_suffix(".csv")
            logger.warning("Parquet write failed, using CSV fallback: %s", exc)
            df.to_csv(csv_fallback, index=False)
            return csv_fallback
    df.to_csv(output_path, index=False)
    return output_path

