from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.reference_data import refresh_reference_data  # noqa: E402
from moto_app.observability import operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        result = refresh_reference_data(
            db_path=ROOT / "database" / "moto_pipeline_tmp.db",
            source_dir=ROOT / "data" / "campaign rules",
        )
        print(f"Database: {result.db_path}")
        print(f"Refreshed scopes: {', '.join(result.refreshed_scopes)}")
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
