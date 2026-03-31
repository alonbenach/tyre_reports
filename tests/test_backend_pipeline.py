from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from database.tools import DatabasePaths, initialize_database  # noqa: E402
from moto_app.app import run_weekly_pipeline  # noqa: E402
from moto_app.ingest import DuplicateSnapshotError  # noqa: E402
from moto_app.observability import latest_run_status  # noqa: E402
from moto_app.reference_data.service import CoreReferenceStatus  # noqa: E402
from tests.reference_seed import seed_reference_tables  # noqa: E402


FIXTURE_SOURCE = ROOT / "tests" / "fixtures" / "2026-02-10_sample.csv"
MIGRATIONS_DIR = ROOT / "database" / "migrations"


class BackendPipelineTests(unittest.TestCase):
    def _prepare_runtime(self) -> tuple[Path, Path, Path, Path, Path]:
        tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
        tmp_root.mkdir(parents=True, exist_ok=True)
        db_path = tmp_root / "moto_pipeline_test.db"
        source_file = tmp_root / "2026-02-10.csv"
        raw_dir = tmp_root / "raw"
        report_dir = tmp_root / "reports"
        log_dir = tmp_root / "logs"

        shutil.copy2(FIXTURE_SOURCE, source_file)
        initialize_database(
            DatabasePaths(
                db_path=db_path,
                migrations_dir=MIGRATIONS_DIR,
            )
        )
        seed_reference_tables(db_path)
        return tmp_root, db_path, source_file, raw_dir, report_dir, log_dir

    def test_weekly_run_records_status_logs_and_outputs(self) -> None:
        tmp_root, db_path, source_file, raw_dir, report_dir, log_dir = self._prepare_runtime()
        try:
            with patch(
                "moto_app.app.service.get_core_reference_status",
                return_value=CoreReferenceStatus(missing_scopes=()),
            ):
                result = run_weekly_pipeline(
                    db_path=db_path,
                    source_file=source_file,
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
            self.assertEqual("2026-02-10", result.snapshot_date)
            self.assertTrue((log_dir / f"{result.run_id}.log").exists())
            self.assertTrue(result.generated_files)
            for path in result.generated_files:
                self.assertTrue(path.exists(), msg=str(path))
            self.assertIn("ingestion", result.stage_durations_seconds)
            self.assertIn("exports", result.stage_summaries)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    def test_duplicate_snapshot_is_blocked_without_replace(self) -> None:
        tmp_root, db_path, source_file, raw_dir, report_dir, log_dir = self._prepare_runtime()
        try:
            with patch(
                "moto_app.app.service.get_core_reference_status",
                return_value=CoreReferenceStatus(missing_scopes=()),
            ):
                first = run_weekly_pipeline(
                    db_path=db_path,
                    source_file=source_file,
                    raw_dir=raw_dir,
                    report_dir=report_dir,
                    log_dir=log_dir,
                    include_pdf=False,
                    replace_snapshot=True,
                    refresh_references=False,
                )
            self.assertEqual("2026-02-10", first.snapshot_date)

            with patch(
                "moto_app.app.service.get_core_reference_status",
                return_value=CoreReferenceStatus(missing_scopes=()),
            ):
                with self.assertRaises(DuplicateSnapshotError):
                    run_weekly_pipeline(
                        db_path=db_path,
                        source_file=source_file,
                        raw_dir=raw_dir,
                        report_dir=report_dir,
                        log_dir=log_dir,
                        include_pdf=False,
                        replace_snapshot=False,
                        refresh_references=False,
                    )
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
