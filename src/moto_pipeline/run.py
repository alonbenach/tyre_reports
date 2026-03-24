from __future__ import annotations

import argparse
import logging

from .ingest import ingest_all_weekly_csv
from .marts import build_gold_marts
from .report_offeror_focus import (
    build_excel_report as build_excel_offeror_report,
)
from .report_offeror_focus import (
    build_pdf_report as build_pdf_offeror_report,
)
from .report_price_offer import (
    build_excel_report as build_excel_positioning_report,
)
from .report_price_offer import (
    build_pdf_report as build_pdf_positioning_report,
)
from .settings import DATA_DIR, GOLD_DIR, RAW_DIR, REPORT_DIR, SILVER_DIR
from .transform import build_motorcycle_silver


def build_logger() -> logging.Logger:
    """Create standard pipeline logger.

    Args:
        None.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("moto_pipeline")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.handlers = [handler]
    return logger


def run_pipeline(
    skip_pdf: bool = False,
    report: str = "both",
) -> None:
    """Run full weekly pipeline from ingest to report generation.

    Args:
        skip_pdf: If True, skip PDF report generation.
        report: Which report flow to run: ``positioning``, ``offeror`` or ``both``.

    Returns:
        None.
    """
    logger = build_logger()
    logger.info("Starting weekly moto report pipeline")
    logger.info("Data dir: %s", DATA_DIR)

    ingest_all_weekly_csv(logger, input_dir=DATA_DIR, raw_dir=RAW_DIR)
    silver_file = build_motorcycle_silver(logger, raw_dir=RAW_DIR, silver_dir=SILVER_DIR)
    build_gold_marts(logger, silver_file=silver_file, gold_dir=GOLD_DIR)
    run_positioning = report in {"positioning", "both"}
    run_offeror = report in {"offeror", "both"}

    if run_positioning:
        build_excel_positioning_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)
        if not skip_pdf:
            build_pdf_positioning_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)

    if run_offeror:
        build_excel_offeror_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)
        if not skip_pdf:
            build_pdf_offeror_report(logger, gold_dir=GOLD_DIR, report_dir=REPORT_DIR)

    logger.info("Pipeline completed successfully")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    Args:
        None.

    Returns:
        Parsed CLI namespace.
    """
    parser = argparse.ArgumentParser(description="Run weekly motorcycle report pipeline.")
    parser.add_argument("--skip-pdf", action="store_true", help="Build Excel only.")
    parser.add_argument(
        "--report",
        choices=["positioning", "offeror", "both"],
        default="both",
        help="Choose report output flow. Default runs both reports in sequence.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for weekly pipeline run.

    Args:
        None.

    Returns:
        None.
    """
    args = parse_args()
    run_pipeline(skip_pdf=args.skip_pdf, report=args.report)


if __name__ == "__main__":
    main()
