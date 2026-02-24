from pathlib import Path
import argparse
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_pipeline.report_price_offer import build_excel_report, build_pdf_report  # noqa: E402
from moto_pipeline.settings import GOLD_DIR, REPORT_DIR  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-pdf", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("moto_pipeline_report")
    build_excel_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)
    if not args.skip_pdf:
        build_pdf_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)

