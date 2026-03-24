from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError
from moto_pipeline.canonical import (
    assert_high_confidence_token_integrity,
    match_to_canonical,
)


def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype("string")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def rim_group(rim_value: pd.Series) -> pd.Series:
    rim_num = pd.to_numeric(rim_value, errors="coerce")
    out = pd.Series("Unknown", index=rim_value.index, dtype="string")
    out = out.mask(rim_num <= 13, "<=13")
    out = out.mask((rim_num >= 14) & (rim_num <= 16), "14-16")
    out = out.mask(rim_num == 17, "17")
    out = out.mask(rim_num == 18, "18")
    out = out.mask(rim_num >= 19, "19+")
    return out


def fitment_position(name_col: pd.Series) -> pd.Series:
    name_norm = normalize_text(name_col).str.upper()
    out = pd.Series("Unknown", index=name_col.index, dtype="string")
    out = out.mask(name_norm.str.contains("FRONT", na=False), "Front")
    out = out.mask(name_norm.str.contains("REAR", na=False), "Rear")
    return out


def pattern_family(name_col: pd.Series) -> pd.Series:
    name_norm = normalize_text(name_col).str.upper()
    tokens = name_norm.str.split(" ", expand=True).iloc[:, :3].fillna("")
    return (tokens[0] + " " + tokens[1] + " " + tokens[2]).str.strip()


def current_utc_year() -> int:
    return int(pd.Timestamp.now(tz="UTC").year)


@dataclass(frozen=True)
class SilverBuildResult:
    db_path: Path
    snapshot_date: str
    silver_rows: int


SILVER_INSERT_COLUMNS = [
    "run_id",
    "snapshot_date",
    "iso_year",
    "iso_week",
    "brand",
    "production_year",
    "seller_norm",
    "product_code",
    "EAN",
    "price_pln",
    "stock_qty",
    "size_norm",
    "rim_num",
    "rim_group",
    "season",
    "fitment_position",
    "pattern_family",
    "name_norm",
    "size_root",
    "pattern_set",
    "segment_reference_group",
    "key_fitments",
    "match_method",
    "pattern_match_score",
    "is_canonical_match",
    "is_high_confidence_match",
    "list_price",
    "ipcode",
    "is_extra_3pct_set",
    "extra_discount",
    "opon_all_in_discount",
    "effective_all_in_discount",
    "expected_net_price_from_list",
    "discount_vs_list_implied",
    "date",
    "built_at_utc",
]


def _load_stage_snapshot(connection: sqlite3.Connection, snapshot_date: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT *
        FROM stg_weekly_source_motorcycle
        WHERE snapshot_date = ?
        ORDER BY source_row_number
        """,
        connection,
        params=[snapshot_date],
    )


def _load_canonical_reference(connection: sqlite3.Connection) -> tuple[pd.DataFrame, float]:
    mapping = pd.read_sql_query(
        """
        SELECT brand, pattern_set, pattern_set_norm, segment_reference_group,
               key_fitments, size_text, size_root
        FROM ref_canonical_fitment_mapping
        """,
        connection,
    ).drop_duplicates()

    price_list = pd.read_sql_query(
        """
        SELECT brand, size_root, pattern_norm, list_price, ipcode
        FROM ref_price_list
        """,
        connection,
    )

    extras = pd.read_sql_query(
        """
        SELECT pattern_set_norm, extra_discount
        FROM ref_campaign_pattern_extras
        """,
        connection,
    ).drop_duplicates()
    if extras.empty:
        extras = pd.DataFrame(columns=["pattern_set_norm", "extra_discount"])

    all_in_row = connection.execute(
        """
        SELECT all_in_discount
        FROM ref_campaign_customer_discounts
        WHERE UPPER(customer) LIKE '%PLATFORMA OPON%'
        ORDER BY imported_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    all_in_discount = float(all_in_row[0]) if all_in_row and all_in_row[0] is not None else 0.0

    ref = mapping.merge(
        price_list[["brand", "size_root", "pattern_norm", "list_price", "ipcode"]],
        left_on=["brand", "size_root", "pattern_set_norm"],
        right_on=["brand", "size_root", "pattern_norm"],
        how="left",
    )
    ref = ref.drop(columns=["pattern_norm"])
    ref = ref.merge(extras, on="pattern_set_norm", how="left")
    ref["extra_discount"] = pd.to_numeric(ref["extra_discount"], errors="coerce").fillna(0.0)
    ref["is_extra_3pct_set"] = ref["extra_discount"] >= 0.03 - 1e-9
    ref["opon_all_in_discount"] = all_in_discount
    ref["opon_all_in_plus_extra"] = ref["opon_all_in_discount"] + ref["extra_discount"]
    ref = (
        ref.groupby(
            ["brand", "pattern_set", "pattern_set_norm", "segment_reference_group", "key_fitments", "size_text", "size_root"],
            dropna=False,
            as_index=False,
        )
        .agg(
            list_price=("list_price", "median"),
            ipcode=("ipcode", "first"),
            extra_discount=("extra_discount", "max"),
            is_extra_3pct_set=("is_extra_3pct_set", "max"),
            opon_all_in_discount=("opon_all_in_discount", "max"),
            opon_all_in_plus_extra=("opon_all_in_plus_extra", "max"),
        )
    )
    return ref, all_in_discount


def _build_silver_frame(stage_df: pd.DataFrame, canonical_ref: pd.DataFrame, all_in_discount: float) -> pd.DataFrame:
    if stage_df.empty:
        return pd.DataFrame()

    moto = stage_df.copy()
    current_year = current_utc_year()
    allowed_production_years = {current_year, current_year - 1}

    moto["production_year"] = pd.to_numeric(moto["productionYear"], errors="coerce").astype("Int64")
    moto = moto[moto["production_year"].isin(allowed_production_years)].copy()
    moto["brand"] = normalize_text(moto["producer"])
    moto["seller_norm"] = normalize_text(moto["seller"])
    moto["size_norm"] = normalize_text(moto["size"])
    moto["name_norm"] = normalize_text(moto["name"])
    moto["price_pln"] = pd.to_numeric(
        moto["price"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    )
    moto["stock_qty"] = pd.to_numeric(moto["amount"], errors="coerce")
    moto["rim_num"] = pd.to_numeric(moto["rim"], errors="coerce")
    moto["rim_group"] = rim_group(moto["rim"])
    moto["fitment_position"] = fitment_position(moto["name"])
    moto["pattern_family"] = pattern_family(moto["name"])
    moto["snapshot_date"] = pd.to_datetime(moto["snapshot_date"], errors="coerce")
    iso = moto["snapshot_date"].dt.isocalendar()
    moto["iso_year"] = iso["year"]
    moto["iso_week"] = iso["week"]

    key_cols = ["snapshot_date", "product_code", "seller_norm", "price_pln"]
    moto = moto.drop_duplicates(subset=key_cols, keep="first")

    matched = match_to_canonical(moto, canonical_ref)
    assert_high_confidence_token_integrity(matched)
    matched["opon_all_in_discount"] = all_in_discount
    matched["effective_all_in_discount"] = matched["opon_all_in_plus_extra"].fillna(all_in_discount)
    matched["expected_net_price_from_list"] = matched["list_price"] * (1 - matched["effective_all_in_discount"])
    matched["discount_vs_list_implied"] = 1 - (matched["price_pln"] / matched["list_price"])

    ordered = [
        "snapshot_date",
        "iso_year",
        "iso_week",
        "brand",
        "production_year",
        "seller_norm",
        "product_code",
        "EAN",
        "price_pln",
        "stock_qty",
        "size_norm",
        "rim_num",
        "rim_group",
        "season",
        "fitment_position",
        "pattern_family",
        "name_norm",
        "size_root",
        "pattern_set",
        "segment_reference_group",
        "key_fitments",
        "match_method",
        "pattern_match_score",
        "is_canonical_match",
        "is_high_confidence_match",
        "list_price",
        "ipcode",
        "is_extra_3pct_set",
        "extra_discount",
        "opon_all_in_discount",
        "effective_all_in_discount",
        "expected_net_price_from_list",
        "discount_vs_list_implied",
        "date",
    ]
    silver = matched[ordered].sort_values(["snapshot_date", "brand", "seller_norm", "product_code"])
    silver["built_at_utc"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    return silver


def build_silver_snapshot(
    db_path: Path,
    snapshot_date: str,
    run_id: str | None = None,
    replace_snapshot: bool = True,
) -> SilverBuildResult:
    with connect_sqlite(db_path) as connection:
        stage_df = _load_stage_snapshot(connection, snapshot_date)
        if stage_df.empty:
            raise OperatorFacingError(
                f"No staged motorcycle rows were found for snapshot {snapshot_date}. Run ingestion for this week before building silver."
            )

        canonical_ref, all_in_discount = _load_canonical_reference(connection)
        silver = _build_silver_frame(stage_df, canonical_ref, all_in_discount)
        if silver.empty:
            raise OperatorFacingError(
                f"The silver build produced no rows for snapshot {snapshot_date}. Check the staging data, reference mappings, and production-year filters."
            )

        if replace_snapshot:
            connection.execute(
                "DELETE FROM silver_motorcycle_weekly WHERE snapshot_date = ?",
                (snapshot_date,),
            )

        records = silver.where(silver.notna(), None).to_dict(orient="records")
        rows = [
            (
                run_id,
                str(record["snapshot_date"])[:10] if record["snapshot_date"] else None,
                int(record["iso_year"]) if record["iso_year"] is not None else None,
                int(record["iso_week"]) if record["iso_week"] is not None else None,
                record["brand"],
                int(record["production_year"]) if record["production_year"] is not None else None,
                record["seller_norm"],
                record["product_code"],
                record["EAN"],
                float(record["price_pln"]) if record["price_pln"] is not None else None,
                float(record["stock_qty"]) if record["stock_qty"] is not None else None,
                record["size_norm"],
                float(record["rim_num"]) if record["rim_num"] is not None else None,
                record["rim_group"],
                record["season"],
                record["fitment_position"],
                record["pattern_family"],
                record["name_norm"],
                record["size_root"],
                record["pattern_set"],
                record["segment_reference_group"],
                record["key_fitments"],
                record["match_method"],
                float(record["pattern_match_score"]) if record["pattern_match_score"] is not None else None,
                int(bool(record["is_canonical_match"])) if record["is_canonical_match"] is not None else None,
                int(bool(record["is_high_confidence_match"])) if record["is_high_confidence_match"] is not None else None,
                float(record["list_price"]) if record["list_price"] is not None else None,
                record["ipcode"],
                int(bool(record["is_extra_3pct_set"])) if record["is_extra_3pct_set"] is not None else None,
                float(record["extra_discount"]) if record["extra_discount"] is not None else None,
                float(record["opon_all_in_discount"]) if record["opon_all_in_discount"] is not None else None,
                float(record["effective_all_in_discount"]) if record["effective_all_in_discount"] is not None else None,
                float(record["expected_net_price_from_list"]) if record["expected_net_price_from_list"] is not None else None,
                float(record["discount_vs_list_implied"]) if record["discount_vs_list_implied"] is not None else None,
                record["date"],
                record["built_at_utc"],
            )
            for record in records
        ]
        silver_columns_sql = ",\n                ".join(SILVER_INSERT_COLUMNS)
        silver_placeholders_sql = ", ".join("?" for _ in SILVER_INSERT_COLUMNS)

        connection.executemany(
            f"""
            INSERT INTO silver_motorcycle_weekly (
                {silver_columns_sql}
            )
            VALUES ({silver_placeholders_sql})
            """,
            rows,
        )
        connection.commit()

    return SilverBuildResult(db_path=db_path, snapshot_date=snapshot_date, silver_rows=len(rows))
