from __future__ import annotations

import hashlib
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from moto_app.db.runtime import connect_sqlite


APP_VERSION = "0.1.0-dev"


class OperatorFacingError(RuntimeError):
    """Exception with a human-readable message intended for operators."""

    def __init__(self, operator_message: str, *, cause: Exception | None = None) -> None:
        super().__init__(operator_message)
        self.operator_message = operator_message
        self.__cause__ = cause


@dataclass(frozen=True)
class RunContext:
    run_id: str
    db_path: Path
    log_path: Path
    snapshot_date: str | None
    logger_name: str


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    snapshot_date: str | None
    status: str
    run_started_at_utc: str
    run_finished_at_utc: str | None
    source_file_name: str | None
    error_message: str | None


@dataclass(frozen=True)
class LatestRunStatus:
    run_id: str | None
    status: str | None
    snapshot_date: str | None
    run_started_at_utc: str | None
    run_finished_at_utc: str | None
    error_message: str | None


@dataclass(frozen=True)
class YearCoverage:
    iso_year: int
    weeks_present: tuple[int, ...]
    latest_week: int
    coverage_percent: int


def _file_sha256(file_path: Path | None) -> str | None:
    if file_path is None or not file_path.exists():
        return None
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _schema_version(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT version
        FROM schema_migrations
        ORDER BY applied_at_utc DESC, version DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _logger_for_run(log_path: Path, logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class RunTracker:
    def __init__(self, db_path: Path, log_dir: Path) -> None:
        self.db_path = Path(db_path)
        self.log_dir = Path(log_dir)

    def start_run(
        self,
        *,
        snapshot_date: str | None,
        report_mode: str,
        skip_pdf: bool,
        source_file: Path | None = None,
    ) -> RunContext:
        run_id = str(uuid.uuid4())
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"{run_id}.log"
        logger_name = f"moto_app.run.{run_id}"
        logger = _logger_for_run(log_path, logger_name)
        logger.info("Run started.")

        with connect_sqlite(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id,
                    snapshot_date,
                    run_started_at_utc,
                    status,
                    report_mode,
                    skip_pdf,
                    source_file_name,
                    source_file_sha256,
                    app_version,
                    schema_version
                )
                VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    snapshot_date,
                    "running",
                    report_mode,
                    1 if skip_pdf else 0,
                    source_file.name if source_file else None,
                    _file_sha256(source_file),
                    APP_VERSION,
                    _schema_version(connection),
                ),
            )
            connection.commit()

        return RunContext(
            run_id=run_id,
            db_path=self.db_path,
            log_path=log_path,
            snapshot_date=snapshot_date,
            logger_name=logger_name,
        )

    def logger(self, context: RunContext) -> logging.Logger:
        return logging.getLogger(context.logger_name)

    def log_step(self, context: RunContext, step_name: str, message: str) -> None:
        self.logger(context).info("[%s] %s", step_name, message)

    def mark_succeeded(self, context: RunContext) -> None:
        self.logger(context).info("Run completed successfully.")
        with connect_sqlite(self.db_path) as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET status = 'succeeded',
                    run_finished_at_utc = datetime('now'),
                    error_message = NULL
                WHERE run_id = ?
                """,
                (context.run_id,),
            )
            connection.commit()

    def mark_failed(self, context: RunContext, exc: Exception) -> None:
        operator_message = operator_message_for_exception(exc)
        self.logger(context).exception("Run failed: %s", operator_message)
        with connect_sqlite(self.db_path) as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET status = 'failed',
                    run_finished_at_utc = datetime('now'),
                    error_message = ?
                WHERE run_id = ?
                """,
                (operator_message, context.run_id),
            )
            connection.commit()


def operator_message_for_exception(exc: Exception) -> str:
    if isinstance(exc, OperatorFacingError):
        return exc.operator_message

    text = str(exc).strip()
    if isinstance(exc, FileNotFoundError):
        return f"Required file not found. {text}"
    if isinstance(exc, PermissionError):
        return "The process could not access a required file or folder. Close any open files and try again."
    if isinstance(exc, ValueError) and text:
        return text
    return "The pipeline hit an unexpected error. Check the run log for technical details and retry the operation."


def list_runs(db_path: Path, limit: int = 20) -> list[RunSummary]:
    with connect_sqlite(db_path) as connection:
        rows = connection.execute(
            """
            SELECT run_id, snapshot_date, status, run_started_at_utc,
                   run_finished_at_utc, source_file_name, error_message
            FROM pipeline_runs
            ORDER BY run_started_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        RunSummary(
            run_id=str(row[0]),
            snapshot_date=row[1],
            status=str(row[2]),
            run_started_at_utc=str(row[3]),
            run_finished_at_utc=row[4],
            source_file_name=row[5],
            error_message=row[6],
        )
        for row in rows
    ]


def latest_run_status(db_path: Path) -> LatestRunStatus:
    with connect_sqlite(db_path) as connection:
        row = connection.execute(
            """
            SELECT run_id, status, snapshot_date, run_started_at_utc,
                   run_finished_at_utc, error_message
            FROM pipeline_runs
            ORDER BY run_started_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return LatestRunStatus(None, None, None, None, None, None)
    return LatestRunStatus(
        run_id=row[0],
        status=row[1],
        snapshot_date=row[2],
        run_started_at_utc=row[3],
        run_finished_at_utc=row[4],
        error_message=row[5],
    )


def list_year_coverage(db_path: Path, *, years: tuple[int, ...]) -> list[YearCoverage]:
    if not years:
        return []
    placeholders = ", ".join("?" for _ in years)
    with connect_sqlite(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT iso_year, iso_week
            FROM silver_motorcycle_weekly
            WHERE iso_year IN ({placeholders})
            GROUP BY iso_year, iso_week
            ORDER BY iso_year DESC, iso_week ASC
            """,
            years,
        ).fetchall()
    weeks_by_year: dict[int, list[int]] = {year: [] for year in years}
    for iso_year, iso_week in rows:
        if iso_year is None or iso_week is None:
            continue
        weeks_by_year.setdefault(int(iso_year), []).append(int(iso_week))
    coverage: list[YearCoverage] = []
    for year in years:
        weeks = sorted(set(weeks_by_year.get(year, [])))
        if not weeks:
            continue
        coverage.append(
            YearCoverage(
                iso_year=year,
                weeks_present=tuple(weeks),
                latest_week=max(weeks),
                coverage_percent=round((len(weeks) / 52) * 100),
            )
        )
    return coverage
