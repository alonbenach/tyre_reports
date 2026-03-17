from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.marts import build_gold_marts  # noqa: E402
from moto_app.observability import operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        result = build_gold_marts(ROOT / "database" / "moto_pipeline_tmp.db")
        print(f"Database: {result.db_path}")
        for table_name, row_count in result.rows_by_table.items():
            print(f"{table_name}: {row_count}")
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
