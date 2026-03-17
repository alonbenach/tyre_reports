from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.exports import (  # noqa: E402
    export_offeror_focus_reports,
    export_positioning_reports,
)
from moto_app.observability import operator_message_for_exception  # noqa: E402


def main() -> None:
    try:
        db_path = ROOT / "database" / "moto_pipeline_tmp.db"
        report_dir = ROOT / "reports"
        positioning = export_positioning_reports(
            db_path=db_path,
            report_dir=report_dir,
            include_pdf=False,
        )
        offeror = export_offeror_focus_reports(
            db_path=db_path,
            report_dir=report_dir,
            include_pdf=False,
        )
        print("Positioning files:")
        for path in positioning.generated_files:
            print(path)
        print("Offeror files:")
        for path in offeror.generated_files:
            print(path)
    except Exception as exc:
        print(operator_message_for_exception(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
