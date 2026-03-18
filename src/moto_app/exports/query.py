from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from moto_app.db.runtime import connect_sqlite


@dataclass(frozen=True)
class GeneratedReportSummary:
    report_id: str
    run_id: str | None
    snapshot_date: str | None
    report_type: str
    format: str
    output_path: Path
    generated_at_utc: str
    status: str


def list_generated_reports(
    db_path: Path,
    limit: int = 20,
    report_type: str | None = None,
) -> list[GeneratedReportSummary]:
    with connect_sqlite(db_path) as connection:
        if report_type is None:
            rows = connection.execute(
                """
                SELECT report_id, run_id, snapshot_date, report_type, format,
                       output_path, generated_at_utc, status
                FROM generated_reports
                ORDER BY generated_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT report_id, run_id, snapshot_date, report_type, format,
                       output_path, generated_at_utc, status
                FROM generated_reports
                WHERE report_type = ?
                ORDER BY generated_at_utc DESC
                LIMIT ?
                """,
                (report_type, limit),
            ).fetchall()
    return [
        GeneratedReportSummary(
            report_id=str(row[0]),
            run_id=row[1],
            snapshot_date=row[2],
            report_type=str(row[3]),
            format=str(row[4]),
            output_path=Path(row[5]),
            generated_at_utc=str(row[6]),
            status=str(row[7]),
        )
        for row in rows
    ]


def list_current_generated_reports(
    db_path: Path,
    *,
    report_type: str | None = None,
) -> list[GeneratedReportSummary]:
    rows = list_generated_reports(db_path, limit=500, report_type=report_type)
    current_by_path: dict[Path, GeneratedReportSummary] = {}
    for row in rows:
        normalized_path = row.output_path.resolve()
        if not normalized_path.exists():
            continue
        normalized_row = GeneratedReportSummary(
            report_id=row.report_id,
            run_id=row.run_id,
            snapshot_date=row.snapshot_date,
            report_type=row.report_type,
            format=row.format,
            output_path=normalized_path,
            generated_at_utc=row.generated_at_utc,
            status=row.status,
        )
        current_by_path.setdefault(normalized_path, normalized_row)
    return sorted(
        current_by_path.values(),
        key=lambda item: (item.snapshot_date or "", item.format, item.output_path.name.lower()),
        reverse=True,
    )
