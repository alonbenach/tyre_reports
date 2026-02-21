from pathlib import Path
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_pipeline.ingest import ingest_all_weekly_csv  # noqa: E402
from moto_pipeline.settings import DATA_DIR, RAW_DIR  # noqa: E402


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("moto_pipeline_ingest")
    ingest_all_weekly_csv(logger, input_dir=DATA_DIR, raw_dir=RAW_DIR)

