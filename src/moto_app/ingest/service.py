from __future__ import annotations

import csv
import hashlib
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError

MOTORCYCLE_TYPE = "Motocykle"
INPUT_COLUMNS = [
    "product_code",
    "EAN",
    "price",
    "price €",
    "amount",
    "realizationTime",
    "productionYear",
    "seller",
    "actualization",
    "is_retreaded",
    "producer",
    "size",
    "width",
    "rim",
    "profil",
    "speed",
    "capacity",
    "season",
    "ROF",
    "XL",
    "name",
    "type",
    "date",
]

STAGE_INSERT_COLUMNS = [
    "run_id",
    "snapshot_date",
    "source_row_number",
    "product_code",
    "EAN",
    "price",
    "price_eur",
    "amount",
    "realizationTime",
    "productionYear",
    "seller",
    "actualization",
    "is_retreaded",
    "producer",
    "size",
    "width",
    "rim",
    "profil",
    "speed",
    "capacity",
    "season",
    "ROF",
    "XL",
    "name",
    "type",
    "date",
    "imported_at_utc",
]


class DuplicateSnapshotError(OperatorFacingError):
    """Raised when a snapshot already exists and replace mode is not enabled."""


@dataclass(frozen=True)
class IngestionResult:
    db_path: Path
    snapshot_date: str
    import_id: str
    archived_file_path: Path
    row_count_total: int
    row_count_motorcycle: int
    duplicate_policy: str


def remove_staged_intake_file(intake_dir: Path, snapshot_date: str) -> Path:
    target_path = intake_dir / f"{snapshot_date}.csv"
    if not target_path.exists():
        raise OperatorFacingError(
            f"The staged intake file for snapshot {snapshot_date} could not be found in {intake_dir}."
        )
    target_path.unlink()
    return target_path


def duplicate_snapshot_message(db_path: Path, source_file: Path) -> str | None:
    snapshot_date = _parse_snapshot_date(source_file)
    source_sha256 = _file_sha256(source_file)
    with connect_sqlite(db_path) as connection:
        exists, existing_sha256 = _existing_snapshot_info(connection, snapshot_date)
    if not exists:
        return None
    if existing_sha256 == source_sha256:
        return (
            f"Snapshot {snapshot_date} is already loaded from the same CSV. "
            "Enable 'Replace snapshot if it already exists' only if you intentionally want to rebuild that week."
        )
    return (
        f"Snapshot {snapshot_date} already exists in the database. "
        "Enable 'Replace snapshot if it already exists' to rebuild that week with the selected staged file."
    )


def _file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_snapshot_date(file_path: Path) -> str:
    return file_path.stem


def _validate_source_file(file_path: Path) -> tuple[str, list[str]]:
    if not file_path.exists():
        raise OperatorFacingError(
            f"The weekly CSV could not be found: {file_path}"
        )
    if file_path.suffix.lower() != ".csv":
        raise OperatorFacingError(
            f"The selected input must be a CSV file. Received: {file_path.name}"
        )

    snapshot_date = _parse_snapshot_date(file_path)
    header = pd.read_csv(
        file_path,
        sep=";",
        nrows=0,
        encoding="utf-8",
        encoding_errors="replace",
    )
    missing = [column for column in INPUT_COLUMNS if column not in header.columns]
    if missing:
        raise OperatorFacingError(
            f"The weekly CSV is missing required columns: {missing}. Export the file again from Platforma Opon and retry."
        )
    return snapshot_date, list(header.columns)


def _copy_to_raw_snapshot(src_file: Path, raw_dir: Path, snapshot_date: str) -> Path:
    target_dir = raw_dir / f"snapshot_date={snapshot_date}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "source.csv"
    shutil.copy2(src_file, target_file)
    return target_file


def _count_rows(file_path: Path) -> tuple[int, int]:
    total_rows = 0
    moto_rows = 0
    with file_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            total_rows += 1
            if row.get("type") == MOTORCYCLE_TYPE:
                moto_rows += 1
    return total_rows, moto_rows


def _existing_snapshot_info(connection: sqlite3.Connection, snapshot_date: str) -> tuple[bool, str | None]:
    row = connection.execute(
        """
        SELECT COUNT(*), MAX(sha256)
        FROM source_imports
        WHERE snapshot_date = ?
        """,
        (snapshot_date,),
    ).fetchone()
    return (bool(row[0]), row[1] if row else None)


def _replace_snapshot(connection: sqlite3.Connection, snapshot_date: str) -> None:
    connection.execute(
        "DELETE FROM stg_weekly_source_motorcycle WHERE snapshot_date = ?",
        (snapshot_date,),
    )
    connection.execute(
        "DELETE FROM source_imports WHERE snapshot_date = ?",
        (snapshot_date,),
    )


def _read_motorcycle_rows(file_path: Path, snapshot_date: str) -> list[tuple]:
    df = pd.read_csv(
        file_path,
        sep=";",
        usecols=INPUT_COLUMNS,
        dtype="string",
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )
    df = df.loc[df["type"] == MOTORCYCLE_TYPE].copy()
    df = df.where(df.notna(), None)
    imported_at = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    rows: list[tuple] = []
    for source_row_number, row in enumerate(df.to_dict(orient="records"), start=1):
        rows.append(
            (
                snapshot_date,
                source_row_number,
                row.get("product_code"),
                row.get("EAN"),
                row.get("price"),
                row.get("price €"),
                row.get("amount"),
                row.get("realizationTime"),
                row.get("productionYear"),
                row.get("seller"),
                row.get("actualization"),
                row.get("is_retreaded"),
                row.get("producer"),
                row.get("size"),
                row.get("width"),
                row.get("rim"),
                row.get("profil"),
                row.get("speed"),
                row.get("capacity"),
                row.get("season"),
                row.get("ROF"),
                row.get("XL"),
                row.get("name"),
                row.get("type"),
                row.get("date"),
                imported_at,
            )
        )
    return rows


def ingest_weekly_csv(
    db_path: Path,
    source_file: Path,
    raw_dir: Path,
    run_id: str | None = None,
    replace_snapshot: bool = False,
) -> IngestionResult:
    snapshot_date, _ = _validate_source_file(source_file)
    raw_dir.mkdir(parents=True, exist_ok=True)
    import_id = str(uuid.uuid4())
    source_sha256 = _file_sha256(source_file)
    row_count_total, row_count_motorcycle = _count_rows(source_file)

    with connect_sqlite(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        exists, existing_sha256 = _existing_snapshot_info(connection, snapshot_date)
        if exists and not replace_snapshot:
            if existing_sha256 == source_sha256:
                raise DuplicateSnapshotError(
                    f"Snapshot {snapshot_date} already exists with the same source file checksum."
                )
            raise DuplicateSnapshotError(
                f"Snapshot {snapshot_date} already exists. Use replace mode to reload this week."
            )

        if exists and replace_snapshot:
            _replace_snapshot(connection, snapshot_date)

        archived_file_path = _copy_to_raw_snapshot(source_file, raw_dir, snapshot_date)
        stage_rows = _read_motorcycle_rows(archived_file_path, snapshot_date)
        stage_columns_sql = ",\n                ".join(STAGE_INSERT_COLUMNS)
        stage_placeholders_sql = ", ".join("?" for _ in STAGE_INSERT_COLUMNS)

        connection.executemany(
            f"""
            INSERT INTO stg_weekly_source_motorcycle (
                {stage_columns_sql}
            )
            VALUES ({stage_placeholders_sql})
            """,
            [(run_id, *row) for row in stage_rows],
        )

        connection.execute(
            """
            INSERT INTO source_imports (
                import_id,
                run_id,
                snapshot_date,
                source_file_path,
                archived_file_path,
                sha256,
                row_count_total,
                row_count_motorcycle,
                imported_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                import_id,
                run_id,
                snapshot_date,
                str(source_file),
                str(archived_file_path),
                source_sha256,
                row_count_total,
                row_count_motorcycle,
            ),
        )
        connection.commit()

    return IngestionResult(
        db_path=db_path,
        snapshot_date=snapshot_date,
        import_id=import_id,
        archived_file_path=archived_file_path,
        row_count_total=row_count_total,
        row_count_motorcycle=row_count_motorcycle,
        duplicate_policy="replace" if replace_snapshot else "block",
    )
