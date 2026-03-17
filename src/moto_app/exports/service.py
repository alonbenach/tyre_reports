from __future__ import annotations

import sqlite3
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError
from moto_pipeline.report_offeror_focus import (
    build_excel_report as build_offeror_excel_report,
)
from moto_pipeline.report_offeror_focus import (
    build_pdf_report as build_offeror_pdf_report,
)
from moto_pipeline.report_price_offer import (
    build_excel_report as build_positioning_excel_report,
)
from moto_pipeline.report_price_offer import (
    build_pdf_report as build_positioning_pdf_report,
)


@dataclass(frozen=True)
class ExportResult:
    db_path: Path
    generated_files: list[Path]


class ExportError(OperatorFacingError):
    """Operator-facing export failure."""


GOLD_EXPORT_TABLES = [
    "gold_market_weekly",
    "gold_brand_weekly",
    "gold_segment_weekly",
    "gold_seller_weekly",
    "gold_fitment_weekly",
    "gold_price_positioning_weekly",
    "gold_mapping_match_quality_weekly",
    "gold_keyfitment_checkpoint_weekly",
    "gold_recap_by_brand_weekly",
]


def _latest_snapshot(connection: sqlite3.Connection) -> str:
    row = connection.execute("SELECT MAX(snapshot_date) FROM silver_motorcycle_weekly").fetchone()
    if row is None or row[0] is None:
        raise ExportError(
            "No silver snapshot is available for export. Build silver and gold data before generating reports."
        )
    return str(row[0])


def _write_temp_gold(connection: sqlite3.Connection, gold_dir: Path) -> None:
    gold_dir.mkdir(parents=True, exist_ok=True)
    for table_name in GOLD_EXPORT_TABLES:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)
        internal_id_cols = [col for col in df.columns if col.endswith("_id")]
        built_cols = [col for col in df.columns if col == "built_at_utc"]
        df = df.drop(columns=internal_id_cols + built_cols, errors="ignore")
        out_name = f"{table_name}.csv"
        df.to_csv(gold_dir / out_name, index=False)


def _write_temp_silver(connection: sqlite3.Connection, silver_dir: Path) -> None:
    silver_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_sql_query("SELECT * FROM silver_motorcycle_weekly", connection)
    df = df.drop(columns=["silver_row_id", "run_id", "built_at_utc"], errors="ignore")
    parquet_path = silver_dir / "motorcycle_weekly.parquet"
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception:
        df.to_csv(silver_dir / "motorcycle_weekly.csv", index=False)


def _read_reference_table(connection: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table_name}", connection)


def _record_generated_report(
    connection: sqlite3.Connection,
    run_id: str | None,
    snapshot_date: str,
    report_type: str,
    output_path: Path,
) -> None:
    connection.execute(
        """
        INSERT INTO generated_reports (
            report_id,
            run_id,
            snapshot_date,
            report_type,
            format,
            output_path,
            generated_at_utc,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)
        """,
        (
            str(uuid.uuid4()),
            run_id,
            snapshot_date,
            report_type,
            output_path.suffix.lstrip(".").lower(),
            str(output_path),
            "succeeded",
        ),
    )


def _prepare_work_dirs(db_path: Path) -> tuple[Path, Path, Path]:
    root = db_path.parent
    work_root = root / "_export_work"
    if work_root.exists():
        shutil.rmtree(work_root, ignore_errors=True)
    gold_dir = work_root / "gold"
    silver_dir = work_root / "silver"
    gold_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    return work_root, gold_dir, silver_dir


def export_positioning_reports(
    db_path: Path,
    report_dir: Path,
    run_id: str | None = None,
    include_pdf: bool = True,
) -> ExportResult:
    generated_files: list[Path] = []
    with connect_sqlite(db_path) as connection:
        snapshot_date = _latest_snapshot(connection)
        canonical_mapping = _read_reference_table(
            connection, "ref_canonical_fitment_mapping"
        )
        work_root, gold_dir, silver_dir = _prepare_work_dirs(db_path)
        try:
            _write_temp_gold(connection, gold_dir)
            _write_temp_silver(connection, silver_dir)

            try:
                excel_path = build_positioning_excel_report(
                    logger=_null_logger(),
                    gold_dir=gold_dir,
                    report_dir=report_dir,
                )
                generated_files.append(excel_path)
                _record_generated_report(
                    connection, run_id, snapshot_date, "positioning", excel_path
                )
            except Exception as exc:
                raise ExportError(
                    "Could not generate the Excel positioning report. Check that the latest snapshot, reference data, and report output folder are valid.",
                    cause=exc,
                ) from exc

            if include_pdf:
                try:
                    pdf_path = build_positioning_pdf_report(
                        logger=_null_logger(),
                        gold_dir=gold_dir,
                        report_dir=report_dir,
                        silver_dir=silver_dir,
                        canonical_mapping=canonical_mapping,
                    )
                    generated_files.append(pdf_path)
                    _record_generated_report(
                        connection, run_id, snapshot_date, "positioning", pdf_path
                    )
                except Exception as exc:
                    raise ExportError(
                        "Could not generate the PDF positioning report. Verify that report dependencies and plotting libraries are available.",
                        cause=exc,
                    ) from exc
        finally:
            shutil.rmtree(work_root, ignore_errors=True)
        connection.commit()
    return ExportResult(db_path=db_path, generated_files=generated_files)


def export_offeror_focus_reports(
    db_path: Path,
    report_dir: Path,
    run_id: str | None = None,
    include_pdf: bool = True,
) -> ExportResult:
    generated_files: list[Path] = []
    with connect_sqlite(db_path) as connection:
        snapshot_date = _latest_snapshot(connection)
        canonical_mapping = _read_reference_table(
            connection, "ref_canonical_fitment_mapping"
        )
        customer_discounts = _read_reference_table(
            connection, "ref_campaign_customer_discounts"
        )
        work_root, gold_dir, silver_dir = _prepare_work_dirs(db_path)
        try:
            _write_temp_gold(connection, gold_dir)
            _write_temp_silver(connection, silver_dir)

            try:
                excel_path = build_offeror_excel_report(
                    logger=_null_logger(),
                    gold_dir=gold_dir,
                    report_dir=report_dir,
                    silver_dir=silver_dir,
                    canonical_mapping=canonical_mapping,
                    customer_discounts=customer_discounts,
                )
                generated_files.append(excel_path)
                _record_generated_report(
                    connection, run_id, snapshot_date, "offeror_focus", excel_path
                )
            except Exception as exc:
                raise ExportError(
                    "Could not generate the Excel offeror-focus report. Check that silver data and campaign reference tables are available for the latest snapshot.",
                    cause=exc,
                ) from exc

            if include_pdf:
                try:
                    pdf_path = build_offeror_pdf_report(
                        logger=_null_logger(),
                        gold_dir=gold_dir,
                        report_dir=report_dir,
                        silver_dir=silver_dir,
                        canonical_mapping=canonical_mapping,
                        customer_discounts=customer_discounts,
                    )
                    generated_files.append(pdf_path)
                    _record_generated_report(
                        connection, run_id, snapshot_date, "offeror_focus", pdf_path
                    )
                except Exception as exc:
                    raise ExportError(
                        "Could not generate the PDF offeror-focus report. Verify that plotting support is installed and the latest report data is complete.",
                        cause=exc,
                    ) from exc
        finally:
            shutil.rmtree(work_root, ignore_errors=True)
        connection.commit()
    return ExportResult(db_path=db_path, generated_files=generated_files)


def _null_logger():
    import logging

    logger = logging.getLogger("moto_app.exports")
    logger.handlers = []
    logger.addHandler(logging.NullHandler())
    return logger
