from pathlib import Path
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_pipeline.settings import RAW_DIR, SILVER_DIR  # noqa: E402
from moto_pipeline.transform import build_motorcycle_silver  # noqa: E402


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("moto_pipeline_transform")
    build_motorcycle_silver(logger, raw_dir=RAW_DIR, silver_dir=SILVER_DIR)

