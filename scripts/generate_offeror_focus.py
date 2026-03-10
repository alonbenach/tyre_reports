from pathlib import Path
import argparse
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_pipeline.report_offeror_focus import (  # noqa: E402
    build_excel_report,
    build_pdf_report,
)
from moto_pipeline.settings import GOLD_DIR, REPORT_DIR, SILVER_DIR  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    Args:
        None.

    Returns:
        Parsed CLI namespace.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-pdf", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("moto_pipeline_offeror_report")
    build_excel_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR, silver_dir=SILVER_DIR)
    if not args.skip_pdf:
        build_pdf_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR, silver_dir=SILVER_DIR)
