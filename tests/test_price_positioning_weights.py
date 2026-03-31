from __future__ import annotations

from pathlib import Path

import pandas as pd

from database.tools import DatabasePaths, initialize_database
from moto_app.db.runtime import connect_sqlite
from moto_app.marts.service import _recap_by_brand_weighted_index
from moto_app.marts.service import _load_turnover_weights
from moto_app.marts.service import _price_positioning as app_price_positioning
from moto_pipeline import canonical
from moto_pipeline.marts import _price_positioning as pipeline_price_positioning


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "database" / "migrations"


def test_load_turnover_weights_maps_materials_to_fitment_keys(monkeypatch) -> None:
    turnover_file = ROOT / "tests" / "fixtures" / "turnover report test.xlsx"
    turnover_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(canonical, "find_turnover_file", lambda campaign_dir=None: turnover_file)
    monkeypatch.setattr(
        canonical,
        "load_canonical_mapping",
        lambda mapping_file=canonical.MAPPING_FILE: pd.DataFrame(
            [
                {
                    "brand": "Pirelli",
                    "pattern_set": "Angel GT II",
                    "pattern_set_norm": "ANGEL GT II",
                    "segment_reference_group": "Touring",
                    "key_fitments": "120/70 17 & 180/55 17",
                    "size_text": "180/55 17 R",
                    "size_root": "180/55 17",
                },
                {
                    "brand": "Pirelli",
                    "pattern_set": "Scorpion Rally",
                    "pattern_set_norm": "SCORPION RALLY",
                    "segment_reference_group": "Enduro",
                    "key_fitments": "",
                    "size_text": "90/90 21 C",
                    "size_root": "90/90 21",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        canonical,
        "load_price_list",
        lambda price_list_file=canonical.PRICE_LIST_FILE: pd.DataFrame(
            [
                {
                    "brand": "Pirelli",
                    "pattern_name": "Angel GT II",
                    "pattern_norm": "ANGEL GT II",
                    "size_text": "180/55 17 R",
                    "size_root": "180/55 17",
                    "segment_reference_group": "Touring",
                    "list_price": 700.0,
                    "ipcode": 3112000,
                },
                {
                    "brand": "Pirelli",
                    "pattern_name": "Scorpion Rally",
                    "pattern_norm": "SCORPION RALLY",
                    "size_text": "90/90 21 C",
                    "size_root": "90/90 21",
                    "segment_reference_group": "Enduro",
                    "list_price": 400.0,
                    "ipcode": 3745800,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        canonical.pd,
        "read_excel",
        lambda path: pd.DataFrame(
            [
                {"Material": 3112000, "NETVAL1": 600.0, "QTYBil": 1.0},
                {"Material": 3112000, "NETVAL1": 150.0, "QTYBil": 1.0},
                {"Material": 3745800, "NETVAL1": 200.0, "QTYBil": 4.0},
                {"Material": 9999999, "NETVAL1": 500.0, "QTYBil": 2.0},
            ]
        ),
    )

    try:
        actual = canonical.load_turnover_weights(turnover_file=turnover_file)

        expected = pd.DataFrame(
            [
                {"analysis_fitment_key": "120/70 17 & 180/55 17", "turnover_weight": 750.0},
                {"analysis_fitment_key": "90/90 21", "turnover_weight": 200.0},
            ]
        )
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected, check_dtype=False)
    finally:
        turnover_file.unlink(missing_ok=True)


def test_price_positioning_uses_fitment_weights_for_overall_only() -> None:
    df = pd.DataFrame(
        [
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "A", "brand": "Pirelli", "price_pln": 100.0, "stock_qty": 5},
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "A", "brand": "Michelin", "price_pln": 90.0, "stock_qty": 3},
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "A", "brand": "Bridgestone", "price_pln": 110.0, "stock_qty": 2},
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "B", "brand": "Pirelli", "price_pln": 200.0, "stock_qty": 4},
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "B", "brand": "Michelin", "price_pln": 180.0, "stock_qty": 6},
            {"snapshot_date": "2026-03-31", "analysis_fitment_key": "B", "brand": "Bridgestone", "price_pln": 220.0, "stock_qty": 7},
        ]
    )
    weights = pd.DataFrame(
        [
            {"analysis_fitment_key": "A", "turnover_weight": 1.0},
            {"analysis_fitment_key": "B", "turnover_weight": 3.0},
        ]
    )

    pipeline_actual = pipeline_price_positioning(df, fitment_weights=weights)
    app_actual = app_price_positioning(df, fitment_weights=weights)

    for actual in (pipeline_actual, app_actual):
        fitment_rows = actual[actual["granularity"] == "fitment_size_root"].sort_values("analysis_fitment_key").reset_index(drop=True)
        assert fitment_rows.loc[0, "pirelli_median_price"] == 100.0
        assert fitment_rows.loc[1, "pirelli_median_price"] == 200.0

        overall = actual[actual["granularity"] == "overall"].iloc[0]
        assert overall["analysis_fitment_key"] == "ALL"
        assert overall["pirelli_median_price"] == 175.0
        assert overall["competitor_median_price"] == 175.0
        assert overall["market_median_price"] == 175.0
        assert overall["price_gap_vs_comp"] == 0.0
        assert overall["pirelli_stock_qty"] == 9
        assert overall["competitor_stock_qty"] == 18
        assert overall["market_stock_qty"] == 27

    pd.testing.assert_frame_equal(
        pipeline_actual.sort_values(["granularity", "analysis_fitment_key"]).reset_index(drop=True),
        app_actual.sort_values(["granularity", "analysis_fitment_key"]).reset_index(drop=True),
    )


def test_price_positioning_uses_snapshot_specific_turnover_months() -> None:
    df = pd.DataFrame(
        [
            {"snapshot_date": "2026-02-10", "analysis_fitment_key": "A", "brand": "Pirelli", "price_pln": 100.0, "stock_qty": 1},
            {"snapshot_date": "2026-02-10", "analysis_fitment_key": "A", "brand": "Michelin", "price_pln": 80.0, "stock_qty": 1},
            {"snapshot_date": "2026-02-10", "analysis_fitment_key": "B", "brand": "Pirelli", "price_pln": 200.0, "stock_qty": 1},
            {"snapshot_date": "2026-02-10", "analysis_fitment_key": "B", "brand": "Michelin", "price_pln": 220.0, "stock_qty": 1},
            {"snapshot_date": "2026-03-24", "analysis_fitment_key": "A", "brand": "Pirelli", "price_pln": 100.0, "stock_qty": 1},
            {"snapshot_date": "2026-03-24", "analysis_fitment_key": "A", "brand": "Michelin", "price_pln": 80.0, "stock_qty": 1},
            {"snapshot_date": "2026-03-24", "analysis_fitment_key": "B", "brand": "Pirelli", "price_pln": 200.0, "stock_qty": 1},
            {"snapshot_date": "2026-03-24", "analysis_fitment_key": "B", "brand": "Michelin", "price_pln": 220.0, "stock_qty": 1},
        ]
    )
    weights = pd.DataFrame(
        [
            {"snapshot_date": pd.Timestamp("2026-02-10"), "analysis_fitment_key": "A", "turnover_weight": 9.0},
            {"snapshot_date": pd.Timestamp("2026-02-10"), "analysis_fitment_key": "B", "turnover_weight": 1.0},
            {"snapshot_date": pd.Timestamp("2026-03-24"), "analysis_fitment_key": "A", "turnover_weight": 1.0},
            {"snapshot_date": pd.Timestamp("2026-03-24"), "analysis_fitment_key": "B", "turnover_weight": 9.0},
        ]
    )

    actual = app_price_positioning(df, fitment_weights=weights)
    overall = actual[actual["granularity"] == "overall"].sort_values("snapshot_date").reset_index(drop=True)

    assert overall.loc[0, "snapshot_date"] == pd.Timestamp("2026-02-10")
    assert overall.loc[0, "pirelli_median_price"] == 110.0
    assert overall.loc[0, "competitor_median_price"] == 94.0

    assert overall.loc[1, "snapshot_date"] == pd.Timestamp("2026-03-24")
    assert overall.loc[1, "pirelli_median_price"] == 190.0
    assert overall.loc[1, "competitor_median_price"] == 206.0


def test_app_turnover_weights_use_previous_month_for_each_snapshot() -> None:
    work_root = ROOT / "database" / "_test_work" / "turnover-weight-query"
    db_path = work_root / "weights.db"
    try:
        work_root.mkdir(parents=True, exist_ok=True)
        initialize_database(DatabasePaths(db_path=db_path, migrations_dir=MIGRATIONS_DIR))
        with connect_sqlite(db_path) as connection:
            connection.executemany(
                """
                INSERT INTO ref_turnover_weights (
                    reference_version,
                    source_sheet,
                    source_file_name,
                    period_start_date,
                    period_end_date,
                    period_month,
                    analysis_fitment_key,
                    turnover_weight,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("turnover report 01-31.01", "Sheet1", "jan.xlsx", "2026-01-01", "2026-01-31", "2026-01", "A", 10.0, "2026-02-01 00:00:00"),
                    ("turnover report 01-31.01", "Sheet1", "jan.xlsx", "2026-01-01", "2026-01-31", "2026-01", "B", 90.0, "2026-02-01 00:00:00"),
                    ("turnover report 01-28.02", "Sheet1", "feb.xlsx", "2026-02-01", "2026-02-28", "2026-02", "A", 30.0, "2026-03-01 00:00:00"),
                    ("turnover report 01-28.02", "Sheet1", "feb.xlsx", "2026-02-01", "2026-02-28", "2026-02", "B", 70.0, "2026-03-01 00:00:00"),
                ],
            )
            connection.commit()
            actual = _load_turnover_weights(
                connection,
                pd.Series([pd.Timestamp("2026-02-10"), pd.Timestamp("2026-03-24")]),
            ).sort_values(["snapshot_date", "analysis_fitment_key"]).reset_index(drop=True)
        expected = pd.DataFrame(
            [
                {"snapshot_date": pd.Timestamp("2026-02-10"), "analysis_fitment_key": "A", "turnover_weight": 10.0},
                {"snapshot_date": pd.Timestamp("2026-02-10"), "analysis_fitment_key": "B", "turnover_weight": 90.0},
                {"snapshot_date": pd.Timestamp("2026-03-24"), "analysis_fitment_key": "A", "turnover_weight": 30.0},
                {"snapshot_date": pd.Timestamp("2026-03-24"), "analysis_fitment_key": "B", "turnover_weight": 70.0},
            ]
        )
        pd.testing.assert_frame_equal(actual, expected)
    finally:
        if work_root.exists():
            import shutil

            shutil.rmtree(work_root, ignore_errors=True)


def test_recap_page_uses_turnover_weighted_segments() -> None:
    work_root = ROOT / "database" / "_test_work" / "turnover-recap-query"
    db_path = work_root / "weights.db"
    try:
        work_root.mkdir(parents=True, exist_ok=True)
        initialize_database(DatabasePaths(db_path=db_path, migrations_dir=MIGRATIONS_DIR))
        with connect_sqlite(db_path) as connection:
            connection.executemany(
                """
                INSERT INTO ref_canonical_fitment_mapping (
                    reference_version,
                    source_sheet,
                    brand,
                    pattern_set,
                    pattern_set_norm,
                    segment_reference_group,
                    key_fitments,
                    size_text,
                    size_root,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("v1", "mapping", "Pirelli", "A", "A", "G1", "KF1", "120/70 17", "120/70 17", "2026-01-01 00:00:00"),
                    ("v1", "mapping", "Pirelli", "B", "B", "G2", "KF2", "180/55 17", "180/55 17", "2026-01-01 00:00:00"),
                ],
            )
            connection.executemany(
                """
                INSERT INTO ref_turnover_weights (
                    reference_version,
                    source_sheet,
                    source_file_name,
                    period_start_date,
                    period_end_date,
                    period_month,
                    analysis_fitment_key,
                    turnover_weight,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("jan", "Sheet1", "jan.xlsx", "2026-01-01", "2026-01-31", "2026-01", "KF1", 90.0, "2026-02-01 00:00:00"),
                    ("jan", "Sheet1", "jan.xlsx", "2026-01-01", "2026-01-31", "2026-01", "KF2", 10.0, "2026-02-01 00:00:00"),
                ],
            )
            connection.commit()

            df = pd.DataFrame(
                [
                    {"snapshot_date": "2026-02-10", "brand": "Pirelli", "segment_reference_group": "G1", "analysis_fitment_key": "KF1", "price_pln": 100.0, "stock_qty": 1, "is_high_confidence_match": True},
                    {"snapshot_date": "2026-02-10", "brand": "Pirelli", "segment_reference_group": "G2", "analysis_fitment_key": "KF2", "price_pln": 200.0, "stock_qty": 1, "is_high_confidence_match": True},
                    {"snapshot_date": "2026-02-10", "brand": "Michelin", "segment_reference_group": "G1", "analysis_fitment_key": "KF1", "price_pln": 110.0, "stock_qty": 1, "is_high_confidence_match": True},
                    {"snapshot_date": "2026-02-10", "brand": "Michelin", "segment_reference_group": "G2", "analysis_fitment_key": "KF2", "price_pln": 300.0, "stock_qty": 1, "is_high_confidence_match": True},
                ]
            )

            recap = _recap_by_brand_weighted_index(df, connection)
            michelin = recap[recap["brand"] == "Michelin"].iloc[0]
            assert round(float(michelin["weighted_brand_price"]), 2) == 129.0
            assert round(float(michelin["weighted_pirelli_price"]), 2) == 110.0
            assert round(float(michelin["positioning_index"]), 2) == round(100 * 129.0 / 110.0, 2)
    finally:
        if work_root.exists():
            import shutil

            shutil.rmtree(work_root, ignore_errors=True)
