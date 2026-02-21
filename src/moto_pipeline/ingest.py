from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from .io import copy_to_raw_snapshot, ensure_dirs, file_sha256
from .settings import DATA_DIR, RAW_DIR


def ingest_all_weekly_csv(
    logger: logging.Logger,
    input_dir: Path = DATA_DIR,
    raw_dir: Path = RAW_DIR,
) -> Path:
    ensure_dirs([raw_dir])

    files = sorted(p for p in input_dir.glob("*.csv") if p.is_file())
    if not files:
        raise FileNotFoundError(f"No weekly csv files found in {input_dir}")

    rows: list[dict[str, str | int]] = []
    for file_path in files:
        copied = copy_to_raw_snapshot(file_path, raw_dir)
        row_count = sum(1 for _ in copied.open("r", encoding="utf-8", errors="replace")) - 1
        rows.append(
            {
                "source_file": str(file_path),
                "raw_file": str(copied),
                "snapshot_date": file_path.stem,
                "sha256": file_sha256(copied),
                "rows": max(row_count, 0),
                "ingested_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            }
        )

    log_path = raw_dir / "ingestion_log.csv"
    pd.DataFrame(rows).sort_values("snapshot_date").to_csv(log_path, index=False)
    logger.info("Ingested %s files into %s", len(rows), raw_dir)
    logger.info("Wrote ingestion metadata: %s", log_path)
    return log_path

