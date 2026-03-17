from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.transform import build_silver_snapshot  # noqa: E402
from moto_app.observability import operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        result = build_silver_snapshot(
            db_path=ROOT / "database" / "moto_pipeline_tmp.db",
            snapshot_date="2026-03-10",
            replace_snapshot=True,
        )
        print(f"Database: {result.db_path}")
        print(f"Snapshot: {result.snapshot_date}")
        print(f"Silver rows: {result.silver_rows}")
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
