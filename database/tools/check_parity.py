from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.testing import collect_parity_results  # noqa: E402


def main() -> None:
    results = collect_parity_results(
        db_path=ROOT / "database" / "moto_pipeline_tmp.db",
        legacy_gold_dir=ROOT / "data" / "gold",
        legacy_silver_path=ROOT / "data" / "silver" / "motorcycle_weekly.parquet",
    )
    failures = [result for result in results if not result.passed]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}: {result.details}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
