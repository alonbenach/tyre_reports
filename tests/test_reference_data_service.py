from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from database.tools import DatabasePaths, initialize_database  # noqa: E402
from moto_app.db.runtime import connect_sqlite  # noqa: E402
from moto_app.reference_data import refresh_reference_data  # noqa: E402


MIGRATIONS_DIR = ROOT / "database" / "migrations"


class ReferenceDataRefreshTests(unittest.TestCase):
    def _prepare_runtime(self) -> tuple[Path, Path, Path]:
        tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
        tmp_root.mkdir(parents=True, exist_ok=True)
        db_path = tmp_root / "moto_pipeline_test.db"
        source_dir = tmp_root / "campaign rules"
        source_dir.mkdir(parents=True, exist_ok=True)

        for name in [
            "canonical fitment mapping.xlsx",
            "price list Pirelli and competitors.xlsx",
            "campaign 2026.xlsx",
        ]:
            (source_dir / name).write_bytes(b"placeholder")

        initialize_database(
            DatabasePaths(
                db_path=db_path,
                migrations_dir=MIGRATIONS_DIR,
            )
        )
        return tmp_root, db_path, source_dir

    def test_refresh_reference_data_persists_rows_and_run_records(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        try:
            mapping = pd.DataFrame(
                [
                    {
                        "brand": "Pirelli",
                        "pattern_set": "Angel GT",
                        "pattern_set_norm": "ANGEL GT",
                        "segment_reference_group": "706 - SUPERSPORT 1st",
                        "key_fitments": "120/70 17 & 180/55 17",
                        "size_text": "120/70 ZR17",
                        "size_root": "120/70 17",
                    }
                ]
            )
            price_list = pd.DataFrame(
                [
                    {
                        "brand": "Pirelli",
                        "pattern_name": "Angel GT",
                        "pattern_norm": "ANGEL GT",
                        "size_text": "120/70 ZR17",
                        "size_root": "120/70 17",
                        "segment_reference_group": "706 - SUPERSPORT 1st",
                        "list_price": 1000.0,
                        "ipcode": "1234567890",
                    }
                ]
            )
            customer_discounts = pd.DataFrame(
                [
                    {
                        "customer": "Platforma Opon",
                        "additional_discount_for_pattern_sets": 0.03,
                        "all_in_discount": 0.25,
                    }
                ]
            )
            pattern_extras = pd.DataFrame(
                [
                    {
                        "pattern_set": "Angel GT",
                        "short_form": "AGT",
                        "pattern_set_norm": "ANGEL GT",
                        "extra_discount": 0.03,
                    }
                ]
            )

            with (
                patch("moto_app.reference_data.service.load_canonical_mapping", return_value=mapping),
                patch("moto_app.reference_data.service.load_price_list", return_value=price_list),
                patch(
                    "moto_app.reference_data.service.load_campaign_customer_discounts",
                    return_value=customer_discounts,
                ),
                patch(
                    "moto_app.reference_data.service._read_campaign_pattern_extras",
                    return_value=pattern_extras,
                ),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                result = refresh_reference_data(db_path=db_path, source_dir=source_dir)

            self.assertEqual(
                ["canonical_mapping", "price_list", "campaign_workbook"],
                result.refreshed_scopes,
            )

            with connect_sqlite(db_path) as connection:
                self.assertEqual(
                    1,
                    connection.execute(
                        "SELECT COUNT(*) FROM ref_canonical_fitment_mapping"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    1,
                    connection.execute("SELECT COUNT(*) FROM ref_price_list").fetchone()[0],
                )
                self.assertEqual(
                    1,
                    connection.execute(
                        "SELECT COUNT(*) FROM ref_campaign_customer_discounts"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    1,
                    connection.execute(
                        "SELECT COUNT(*) FROM ref_campaign_pattern_extras"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    3,
                    connection.execute(
                        "SELECT COUNT(*) FROM reference_refresh_runs WHERE status = 'succeeded'"
                    ).fetchone()[0],
                )
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
