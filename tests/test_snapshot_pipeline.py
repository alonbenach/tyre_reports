from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pandas as pd
import pytest

from database.tools import DatabasePaths, initialize_database
from moto_app.db.runtime import connect_sqlite
from moto_app.ingest import ingest_weekly_csv
from moto_app.marts import build_gold_marts
from moto_app.reference_data import refresh_reference_data
from moto_app.transform import build_silver_snapshot
from tests.snapshot_utils import assert_frame_matches_snapshot


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = ROOT / "tests" / "fixtures" / "2026-02-10_sample.csv"
REFERENCE_DIR = ROOT / "data" / "campaign rules"
MIGRATIONS_DIR = ROOT / "database" / "migrations"


@pytest.fixture
def pipeline_snapshot_db(monkeypatch: pytest.MonkeyPatch) -> Path:
    work_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    db_path = work_root / "snapshot_test.db"
    raw_dir = work_root / "raw"
    staged_fixture = work_root / "2026-02-10.csv"

    try:
        work_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(FIXTURE_CSV, staged_fixture)
        initialize_database(
            DatabasePaths(
                db_path=db_path,
                migrations_dir=MIGRATIONS_DIR,
            )
        )
        refresh_reference_data(db_path=db_path, source_dir=REFERENCE_DIR)

        monkeypatch.setattr("moto_app.transform.service.current_utc_year", lambda: 2026)

        ingest_weekly_csv(
            db_path=db_path,
            source_file=staged_fixture,
            raw_dir=raw_dir,
            replace_snapshot=True,
        )
        build_silver_snapshot(
            db_path=db_path,
            snapshot_date="2026-02-10",
            replace_snapshot=True,
        )
        build_gold_marts(db_path=db_path)
        yield db_path
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


def test_small_fixture_matches_saved_snapshots(pipeline_snapshot_db: Path, update_snapshots: bool) -> None:
    with connect_sqlite(pipeline_snapshot_db) as connection:
        silver = pd.read_sql_query(
            """
            SELECT snapshot_date, brand, production_year, seller_norm, product_code, price_pln,
                   stock_qty, size_root, pattern_set, segment_reference_group, key_fitments,
                   match_method, pattern_match_score, is_high_confidence_match, list_price,
                   extra_discount, effective_all_in_discount
            FROM silver_motorcycle_weekly
            ORDER BY brand, seller_norm, product_code
            """,
            connection,
        )
        gold_brand = pd.read_sql_query(
            """
            SELECT snapshot_date, brand, rows, unique_products, unique_sellers,
                   stock_qty, median_price, mean_price
            FROM gold_brand_weekly
            ORDER BY brand
            """,
            connection,
        )
        gold_positioning = pd.read_sql_query(
            """
            SELECT snapshot_date, granularity, analysis_fitment_key,
                   pirelli_median_price, competitor_median_price, market_median_price,
                   price_gap_vs_comp, pirelli_price_index
            FROM gold_price_positioning_weekly
            ORDER BY granularity, analysis_fitment_key
            """,
            connection,
        )
        row_counts = pd.read_sql_query(
            """
            SELECT 'silver_motorcycle_weekly' AS dataset, COUNT(*) AS row_count FROM silver_motorcycle_weekly
            UNION ALL
            SELECT 'gold_brand_weekly' AS dataset, COUNT(*) AS row_count FROM gold_brand_weekly
            UNION ALL
            SELECT 'gold_price_positioning_weekly' AS dataset, COUNT(*) AS row_count FROM gold_price_positioning_weekly
            UNION ALL
            SELECT 'gold_recap_by_brand_weekly' AS dataset, COUNT(*) AS row_count FROM gold_recap_by_brand_weekly
            ORDER BY dataset
            """,
            connection,
        )

    assert_frame_matches_snapshot(
        name="pipeline_fixture/silver_motorcycle_weekly.csv",
        df=silver,
        update_snapshots=update_snapshots,
    )
    assert_frame_matches_snapshot(
        name="pipeline_fixture/gold_brand_weekly.csv",
        df=gold_brand,
        update_snapshots=update_snapshots,
    )
    assert_frame_matches_snapshot(
        name="pipeline_fixture/gold_price_positioning_weekly.csv",
        df=gold_positioning,
        update_snapshots=update_snapshots,
    )
    assert_frame_matches_snapshot(
        name="pipeline_fixture/row_counts.csv",
        df=row_counts,
        update_snapshots=update_snapshots,
    )
