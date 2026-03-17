from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.app import run_weekly_pipeline  # noqa: E402
from moto_app.observability import latest_run_status  # noqa: E402
from moto_app.testing import collect_parity_results  # noqa: E402


class BackendPipelineTests(unittest.TestCase):
    def test_verified_db_matches_legacy_parity_baseline(self) -> None:
        results = collect_parity_results(
            db_path=ROOT / "database" / "moto_pipeline_tmp.db",
            legacy_gold_dir=ROOT / "data" / "gold",
            legacy_silver_path=ROOT / "data" / "silver" / "motorcycle_weekly.parquet",
        )
        failed = [result for result in results if not result.passed]
        self.assertEqual([], failed, msg=[f"{result.name}: {result.details}" for result in failed])

    def test_weekly_run_records_status_logs_and_outputs(self) -> None:
        tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
        try:
            tmp_root.mkdir(parents=True, exist_ok=True)
            db_path = tmp_root / "moto_pipeline_tmp.db"
            shutil.copy2(ROOT / "database" / "moto_pipeline_tmp.db", db_path)
            raw_dir = tmp_root / "raw"
            report_dir = tmp_root / "reports"
            log_dir = tmp_root / "logs"

            result = run_weekly_pipeline(
                db_path=db_path,
                source_file=ROOT / "data" / "2026-03-10.csv",
                raw_dir=raw_dir,
                report_dir=report_dir,
                log_dir=log_dir,
                include_pdf=False,
                replace_snapshot=True,
                refresh_references=False,
            )

            status = latest_run_status(db_path)
            self.assertEqual("succeeded", status.status)
            self.assertEqual(result.run_id, status.run_id)
            self.assertTrue((log_dir / f"{result.run_id}.log").exists())
            self.assertTrue(result.generated_files)
            for path in result.generated_files:
                self.assertTrue(path.exists(), msg=str(path))
            self.assertIn("ingestion", result.stage_durations_seconds)
            self.assertIn("exports", result.stage_summaries)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
