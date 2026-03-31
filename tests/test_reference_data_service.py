from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from datetime import date
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
from moto_app.reference_data import (  # noqa: E402
    get_core_reference_status,
    get_turnover_reference_status,
    refresh_reference_data,
    refresh_turnover_reference_data,
)


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

    def test_core_reference_status_reports_missing_tables_until_refresh(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        try:
            status = get_core_reference_status(db_path)
            self.assertFalse(status.is_ready)

            mapping = pd.DataFrame(
                [{"brand": "Pirelli", "pattern_set": "Angel GT", "pattern_set_norm": "ANGEL GT", "segment_reference_group": "706", "key_fitments": "120/70 17 & 180/55 17", "size_text": "120/70 ZR17", "size_root": "120/70 17"}]
            )
            price_list = pd.DataFrame(
                [{"brand": "Pirelli", "pattern_name": "Angel GT", "pattern_norm": "ANGEL GT", "size_text": "120/70 ZR17", "size_root": "120/70 17", "segment_reference_group": "706", "list_price": 1000.0, "ipcode": "1234567890"}]
            )
            customer_discounts = pd.DataFrame(
                [{"customer": "Platforma Opon", "additional_discount_for_pattern_sets": 0.03, "all_in_discount": 0.25}]
            )
            pattern_extras = pd.DataFrame(
                [{"pattern_set": "Angel GT", "short_form": "AGT", "pattern_set_norm": "ANGEL GT", "extra_discount": 0.03}]
            )
            with (
                patch("moto_app.reference_data.service.load_canonical_mapping", return_value=mapping),
                patch("moto_app.reference_data.service.load_price_list", return_value=price_list),
                patch("moto_app.reference_data.service.load_campaign_customer_discounts", return_value=customer_discounts),
                patch("moto_app.reference_data.service._read_campaign_pattern_extras", return_value=pattern_extras),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                refresh_reference_data(db_path=db_path, source_dir=source_dir)

            status = get_core_reference_status(db_path)
            self.assertTrue(status.is_ready)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    def test_core_reference_status_allows_empty_pattern_extras(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        try:
            mapping = pd.DataFrame(
                [{"brand": "Pirelli", "pattern_set": "Angel GT", "pattern_set_norm": "ANGEL GT", "segment_reference_group": "706", "key_fitments": "120/70 17 & 180/55 17", "size_text": "120/70 ZR17", "size_root": "120/70 17"}]
            )
            price_list = pd.DataFrame(
                [{"brand": "Pirelli", "pattern_name": "Angel GT", "pattern_norm": "ANGEL GT", "size_text": "120/70 ZR17", "size_root": "120/70 17", "segment_reference_group": "706", "list_price": 1000.0, "ipcode": "1234567890"}]
            )
            customer_discounts = pd.DataFrame(
                [{"customer": "Platforma Opon", "additional_discount_for_pattern_sets": 0.03, "all_in_discount": 0.25}]
            )
            empty_pattern_extras = pd.DataFrame(
                columns=["pattern_set", "short_form", "pattern_set_norm", "extra_discount"]
            )
            with (
                patch("moto_app.reference_data.service.load_canonical_mapping", return_value=mapping),
                patch("moto_app.reference_data.service.load_price_list", return_value=price_list),
                patch("moto_app.reference_data.service.load_campaign_customer_discounts", return_value=customer_discounts),
                patch("moto_app.reference_data.service._read_campaign_pattern_extras", return_value=empty_pattern_extras),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                refresh_reference_data(db_path=db_path, source_dir=source_dir)

            with connect_sqlite(db_path) as connection:
                self.assertEqual(
                    0,
                    connection.execute(
                        "SELECT COUNT(*) FROM ref_campaign_pattern_extras"
                    ).fetchone()[0],
                )

            status = get_core_reference_status(db_path)
            self.assertTrue(status.is_ready)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    def test_refresh_turnover_reference_data_persists_weights_and_status(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        turnover_file = source_dir / "turnover report 01-31.03.xlsx"
        turnover_file.write_bytes(b"placeholder")
        try:
            with (
                patch(
                    "moto_app.reference_data.service.load_turnover_weights",
                    return_value=pd.DataFrame(
                        [
                            {"analysis_fitment_key": "120/70 17 & 180/55 17", "turnover_weight": 750.0},
                            {"analysis_fitment_key": "90/90 21", "turnover_weight": 200.0},
                        ]
                    ),
                ),
                patch(
                    "moto_app.reference_data.service.pd.read_excel",
                    return_value=pd.DataFrame(
                        [
                            {"Bill Date": pd.Timestamp("2026-03-01"), "Material": 1, "NETVAL1": 10.0},
                            {"Bill Date": pd.Timestamp("2026-03-31"), "Material": 2, "NETVAL1": 20.0},
                        ]
                    ),
                ),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                result = refresh_turnover_reference_data(db_path=db_path, turnover_file=turnover_file)

            self.assertEqual(["turnover_workbook"], result.refreshed_scopes)

            with connect_sqlite(db_path) as connection:
                self.assertEqual(
                    2,
                    connection.execute("SELECT COUNT(*) FROM ref_turnover_weights").fetchone()[0],
                )
                self.assertEqual(
                    1,
                    connection.execute(
                        "SELECT COUNT(*) FROM reference_refresh_runs WHERE refresh_scope = 'turnover_workbook' AND status = 'succeeded'"
                    ).fetchone()[0],
                )

            status = get_turnover_reference_status(db_path, today_value=date(2026, 4, 4))
            self.assertEqual("2026-03", status.expected_period_month)
            self.assertEqual("2026-03", status.latest_period_month)
            self.assertFalse(status.is_missing_expected_month)
            rerun_status = get_turnover_reference_status(db_path, snapshot_date="2026-02-10")
            self.assertEqual("2026-01", rerun_status.expected_period_month)
            self.assertTrue(rerun_status.is_missing_expected_month)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    def test_refresh_reference_data_imports_turnover_when_workbook_present(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        turnover_file = source_dir / "turnover report 01-31.03.xlsx"
        turnover_file.write_bytes(b"placeholder")
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
                [{"customer": "Platforma Opon", "additional_discount_for_pattern_sets": 0.03, "all_in_discount": 0.25}]
            )
            pattern_extras = pd.DataFrame(
                [{"pattern_set": "Angel GT", "short_form": "AGT", "pattern_set_norm": "ANGEL GT", "extra_discount": 0.03}]
            )

            def fake_read_excel(path, *args, **kwargs):
                if Path(path).name.lower().startswith("turnover report"):
                    return pd.DataFrame(
                        [
                            {"Bill Date": pd.Timestamp("2026-03-01"), "Material": 1, "NETVAL1": 10.0},
                            {"Bill Date": pd.Timestamp("2026-03-31"), "Material": 2, "NETVAL1": 20.0},
                        ]
                    )
                return pd.DataFrame()

            with (
                patch("moto_app.reference_data.service.load_canonical_mapping", return_value=mapping),
                patch("moto_app.reference_data.service.load_price_list", return_value=price_list),
                patch("moto_app.reference_data.service.load_campaign_customer_discounts", return_value=customer_discounts),
                patch("moto_app.reference_data.service._read_campaign_pattern_extras", return_value=pattern_extras),
                patch(
                    "moto_app.reference_data.service.load_turnover_weights",
                    return_value=pd.DataFrame([{"analysis_fitment_key": "120/70 17 & 180/55 17", "turnover_weight": 750.0}]),
                ),
                patch("moto_app.reference_data.service.pd.read_excel", side_effect=fake_read_excel),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                result = refresh_reference_data(db_path=db_path, source_dir=source_dir)

            self.assertEqual(
                ["canonical_mapping", "price_list", "campaign_workbook", "turnover_workbook"],
                result.refreshed_scopes,
            )
            with connect_sqlite(db_path) as connection:
                self.assertEqual(
                    1,
                    connection.execute("SELECT COUNT(*) FROM ref_turnover_weights").fetchone()[0],
                )
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    def test_refresh_turnover_reference_data_rejects_filename_period_mismatch(self) -> None:
        tmp_root, db_path, source_dir = self._prepare_runtime()
        turnover_file = source_dir / "turnover report 2026-03.xlsx"
        turnover_file.write_bytes(b"placeholder")
        try:
            with (
                patch(
                    "moto_app.reference_data.service.load_turnover_weights",
                    return_value=pd.DataFrame([{"analysis_fitment_key": "120/70 17 & 180/55 17", "turnover_weight": 750.0}]),
                ),
                patch(
                    "moto_app.reference_data.service.pd.read_excel",
                    return_value=pd.DataFrame(
                        [
                            {"Bill Date": pd.Timestamp("2026-02-02"), "Material": 1, "NETVAL1": 10.0},
                            {"Bill Date": pd.Timestamp("2026-02-28"), "Material": 2, "NETVAL1": 20.0},
                        ]
                    ),
                ),
                patch("moto_app.reference_data.service._file_sha256", return_value="sha"),
            ):
                with self.assertRaisesRegex(Exception, "filename month does not match"):
                    refresh_turnover_reference_data(db_path=db_path, turnover_file=turnover_file)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
