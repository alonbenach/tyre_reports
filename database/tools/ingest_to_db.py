from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.ingest import ingest_weekly_csv  # noqa: E402
from moto_app.observability import operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        source_file = ROOT / "data" / "2026-03-10.csv"
        result = ingest_weekly_csv(
            db_path=ROOT / "database" / "moto_pipeline_tmp.db",
            source_file=source_file,
            raw_dir=ROOT / "data" / "raw",
            replace_snapshot=False,
        )
        print(f"Database: {result.db_path}")
        print(f"Snapshot: {result.snapshot_date}")
        print(f"Import ID: {result.import_id}")
        print(f"Archived file: {result.archived_file_path}")
        print(f"Rows total: {result.row_count_total}")
        print(f"Rows motorcycle: {result.row_count_motorcycle}")
        print(f"Duplicate policy: {result.duplicate_policy}")
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
