from pathlib import Path
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_pipeline.marts import build_gold_marts  # noqa: E402
from moto_pipeline.settings import GOLD_DIR, SILVER_DIR  # noqa: E402


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("moto_pipeline_marts")
    silver_parquet = SILVER_DIR / "motorcycle_weekly.parquet"
    silver_csv = SILVER_DIR / "motorcycle_weekly.csv"
    silver_file = silver_parquet if silver_parquet.exists() else silver_csv
    build_gold_marts(logger, silver_file=silver_file, gold_dir=GOLD_DIR)

