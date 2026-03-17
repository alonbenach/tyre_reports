from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.app import run_weekly_pipeline  # noqa: E402
from moto_app.config import ensure_runtime_dirs, load_config  # noqa: E402
from moto_app.observability import latest_run_status, operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        config = load_config(ROOT)
        ensure_runtime_dirs(config)
        result = run_weekly_pipeline(
            db_path=config.database_dir / "moto_pipeline_tmp.db",
            source_file=config.data_dir / "2026-03-10.csv",
            raw_dir=config.raw_archive_dir,
            report_dir=config.reports_dir,
            log_dir=config.logs_dir,
            include_pdf=config.include_pdf_by_default,
            replace_snapshot=True,
            refresh_references=False,
        )
        print(f"Run ID: {result.run_id}")
        print(f"Snapshot: {result.snapshot_date}")
        print(f"Silver rows: {result.silver_rows}")
        print("Stage durations (seconds):")
        for stage_name, duration in result.stage_durations_seconds.items():
            print(f"  {stage_name}: {duration}")
        print("Stage summaries:")
        for stage_name, summary in result.stage_summaries.items():
            print(f"  {stage_name}: {summary}")
        print("Gold rows:")
        for table_name, row_count in result.gold_rows_by_table.items():
            print(f"  {table_name}: {row_count}")
        print("Generated files:")
        for path in result.generated_files:
            print(f"  {path}")
        latest_run = latest_run_status(config.database_dir / "moto_pipeline_tmp.db")
        print(f"Run status: {latest_run.status}")
        print(f"Run log: {config.logs_dir / (result.run_id + '.log')}")
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
