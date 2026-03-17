from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from moto_app.exports import ExportResult, export_offeror_focus_reports, export_positioning_reports
from moto_app.ingest import ingest_weekly_csv
from moto_app.marts import build_gold_marts
from moto_app.observability import RunTracker
from moto_app.reference_data import refresh_reference_data
from moto_app.transform import build_silver_snapshot


@dataclass(frozen=True)
class WeeklyRunResult:
    run_id: str
    snapshot_date: str
    silver_rows: int
    gold_rows_by_table: dict[str, int]
    generated_files: list[Path]
    stage_durations_seconds: dict[str, float]
    stage_summaries: dict[str, str]


def run_weekly_pipeline(
    *,
    db_path: Path,
    source_file: Path,
    raw_dir: Path,
    report_dir: Path,
    log_dir: Path,
    include_pdf: bool,
    replace_snapshot: bool,
    refresh_references: bool = False,
    reference_dir: Path | None = None,
) -> WeeklyRunResult:
    snapshot_date = source_file.stem
    tracker = RunTracker(db_path=db_path, log_dir=log_dir)
    context = tracker.start_run(
        snapshot_date=snapshot_date,
        report_mode="excel+pdf" if include_pdf else "excel",
        skip_pdf=not include_pdf,
        source_file=source_file,
    )

    try:
        stage_durations: dict[str, float] = {}
        stage_summaries: dict[str, str] = {}
        if refresh_references:
            step_started = perf_counter()
            if reference_dir is None:
                raise ValueError("Reference refresh was requested, but no reference directory was provided.")
            tracker.log_step(context, "reference_data", "Refreshing SQL-backed reference tables.")
            refresh_result = refresh_reference_data(db_path=db_path, source_dir=reference_dir)
            stage_durations["reference_data"] = round(perf_counter() - step_started, 2)
            stage_summaries["reference_data"] = (
                f"Refreshed scopes: {', '.join(refresh_result.refreshed_scopes)}."
            )
            tracker.log_step(context, "reference_data", stage_summaries["reference_data"])

        step_started = perf_counter()
        tracker.log_step(context, "ingestion", "Validating and loading weekly CSV into staging.")
        ingest_result = ingest_weekly_csv(
            db_path=db_path,
            source_file=source_file,
            raw_dir=raw_dir,
            run_id=context.run_id,
            replace_snapshot=replace_snapshot,
        )
        stage_durations["ingestion"] = round(perf_counter() - step_started, 2)
        stage_summaries["ingestion"] = (
            f"{ingest_result.row_count_motorcycle} motorcycle rows loaded from "
            f"{ingest_result.row_count_total} total source rows."
        )
        tracker.log_step(context, "ingestion", stage_summaries["ingestion"])

        step_started = perf_counter()
        tracker.log_step(context, "transformation", "Building silver snapshot from staged motorcycle rows.")
        silver_result = build_silver_snapshot(
            db_path=db_path,
            snapshot_date=ingest_result.snapshot_date,
            run_id=context.run_id,
            replace_snapshot=True,
        )
        stage_durations["transformation"] = round(perf_counter() - step_started, 2)
        stage_summaries["transformation"] = (
            f"{silver_result.silver_rows} silver rows built for snapshot {silver_result.snapshot_date}."
        )
        tracker.log_step(context, "transformation", stage_summaries["transformation"])

        step_started = perf_counter()
        tracker.log_step(context, "marts", "Rebuilding SQL-backed gold reporting marts.")
        gold_result = build_gold_marts(db_path=db_path)
        stage_durations["marts"] = round(perf_counter() - step_started, 2)
        total_gold_rows = sum(gold_result.rows_by_table.values())
        stage_summaries["marts"] = (
            f"{total_gold_rows} total gold rows rebuilt across {len(gold_result.rows_by_table)} marts."
        )
        tracker.log_step(context, "marts", stage_summaries["marts"])

        step_started = perf_counter()
        tracker.log_step(context, "exports", "Generating management report files from SQL-backed datasets.")
        generated_files: list[Path] = []
        positioning_result: ExportResult = export_positioning_reports(
            db_path=db_path,
            report_dir=report_dir,
            run_id=context.run_id,
            include_pdf=include_pdf,
        )
        generated_files.extend(positioning_result.generated_files)
        offeror_result: ExportResult = export_offeror_focus_reports(
            db_path=db_path,
            report_dir=report_dir,
            run_id=context.run_id,
            include_pdf=include_pdf,
        )
        generated_files.extend(offeror_result.generated_files)
        stage_durations["exports"] = round(perf_counter() - step_started, 2)
        stage_summaries["exports"] = f"{len(generated_files)} report files generated."
        tracker.log_step(context, "exports", stage_summaries["exports"])
        tracker.log_step(context, "timing", f"Stage durations (seconds): {stage_durations}")

        tracker.mark_succeeded(context)
        return WeeklyRunResult(
            run_id=context.run_id,
            snapshot_date=ingest_result.snapshot_date,
            silver_rows=silver_result.silver_rows,
            gold_rows_by_table=gold_result.rows_by_table,
            generated_files=generated_files,
            stage_durations_seconds=stage_durations,
            stage_summaries=stage_summaries,
        )
    except Exception as exc:
        tracker.mark_failed(context, exc)
        raise
