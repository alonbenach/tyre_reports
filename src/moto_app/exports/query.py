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


def list_generated_reports(db_path: Path, limit: int = 20) -> list[GeneratedReportSummary]:
    with connect_sqlite(db_path) as connection:
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
