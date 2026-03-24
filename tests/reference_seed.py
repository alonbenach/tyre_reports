from __future__ import annotations

from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite


ROOT = Path(__file__).resolve().parents[1]
REFERENCE_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "reference"

REFERENCE_TABLE_FIXTURES = {
    "ref_canonical_fitment_mapping": REFERENCE_FIXTURE_DIR / "ref_canonical_fitment_mapping.csv",
    "ref_price_list": REFERENCE_FIXTURE_DIR / "ref_price_list.csv",
    "ref_campaign_customer_discounts": REFERENCE_FIXTURE_DIR / "ref_campaign_customer_discounts.csv",
    "ref_campaign_pattern_extras": REFERENCE_FIXTURE_DIR / "ref_campaign_pattern_extras.csv",
}


def seed_reference_tables(db_path: Path) -> None:
    with connect_sqlite(db_path) as connection:
        for table_name, fixture_path in REFERENCE_TABLE_FIXTURES.items():
            frame = pd.read_csv(fixture_path)
            connection.execute(f"DELETE FROM {table_name}")
            if frame.empty:
                continue
            frame = frame.where(frame.notna(), None)
            columns = list(frame.columns)
            placeholders = ", ".join("?" for _ in columns)
            sql_columns = ", ".join(columns)
            rows = [tuple(record[column] for column in columns) for record in frame.to_dict(orient="records")]
            connection.executemany(
                f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})",
                rows,
            )
        connection.commit()
