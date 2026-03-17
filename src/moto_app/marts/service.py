from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError

FOCUS_BRANDS = ["Pirelli", "Michelin", "Bridgestone", "Dunlop", "Continental"]
TOP_COMPETITORS = ["Michelin", "Bridgestone", "Dunlop", "Continental"]
RECAP_BRANDS = ["Pirelli", "Metzeler", "Michelin", "Continental", "Bridgestone", "Dunlop"]


def _attach_week_offsets(df: pd.DataFrame, value_col: str, keys: list[str]) -> pd.DataFrame:
    base = df.copy()
    base["snapshot_date"] = pd.to_datetime(base["snapshot_date"], errors="coerce")
    iso = base["snapshot_date"].dt.isocalendar()
    base["iso_year"] = iso["year"]
    base["iso_week"] = iso["week"]
    base = base.sort_values(keys + ["iso_year", "iso_week"])
    base[f"{value_col}_prev_week"] = base.groupby(keys, dropna=False)[value_col].shift(1)

    yoy = base[keys + ["iso_year", "iso_week", value_col]].copy()
    yoy["iso_year"] = yoy["iso_year"] + 1
    yoy = yoy.rename(columns={value_col: f"{value_col}_prev_year"})
    joined = base.merge(yoy, on=keys + ["iso_year", "iso_week"], how="left")
    joined[f"{value_col}_wow_delta"] = joined[value_col] - joined[f"{value_col}_prev_week"]
    joined[f"{value_col}_yoy_delta"] = joined[value_col] - joined[f"{value_col}_prev_year"]
    return joined.drop(columns=["iso_year", "iso_week"])


def _analysis_fitment_key(df: pd.DataFrame) -> pd.Series:
    key_fit = df["key_fitments"].astype("string").fillna("").str.strip()
    size_root = df["size_root"].astype("string").fillna("").str.strip()
    out = pd.Series("UNMAPPED", index=df.index, dtype="string")
    out = out.mask(key_fit != "", key_fit)
    out = out.mask((out == "UNMAPPED") & (size_root != ""), size_root)
    return out


def _price_positioning(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["snapshot_date", "analysis_fitment_key"]
    per_brand = (
        df.groupby(group_cols + ["brand"], dropna=False)
        .agg(median_price=("price_pln", "median"), stock_qty=("stock_qty", "sum"))
        .reset_index()
    )

    pirelli = per_brand.loc[per_brand["brand"] == "Pirelli", group_cols + ["median_price", "stock_qty"]].rename(
        columns={"median_price": "pirelli_median_price", "stock_qty": "pirelli_stock_qty"}
    )
    comp = (
        per_brand.loc[per_brand["brand"].isin(TOP_COMPETITORS)]
        .groupby(group_cols, dropna=False)
        .agg(competitor_median_price=("median_price", "median"), competitor_stock_qty=("stock_qty", "sum"))
        .reset_index()
    )
    market = (
        df.groupby(group_cols, dropna=False)
        .agg(market_median_price=("price_pln", "median"), market_stock_qty=("stock_qty", "sum"))
        .reset_index()
    )

    by_fitment = pirelli.merge(comp, on=group_cols, how="left").merge(market, on=group_cols, how="left")
    by_fitment["price_gap_vs_comp"] = by_fitment["pirelli_median_price"] - by_fitment["competitor_median_price"]
    by_fitment["pirelli_price_index"] = 100 * (by_fitment["pirelli_median_price"] / by_fitment["market_median_price"])
    by_fitment["granularity"] = "fitment_size_root"

    overall = (
        df.groupby(["snapshot_date", "brand"], dropna=False)
        .agg(median_price=("price_pln", "median"), stock_qty=("stock_qty", "sum"))
        .reset_index()
    )
    pirelli_overall = overall.loc[overall["brand"] == "Pirelli", ["snapshot_date", "median_price", "stock_qty"]].rename(
        columns={"median_price": "pirelli_median_price", "stock_qty": "pirelli_stock_qty"}
    )
    comp_overall = (
        overall.loc[overall["brand"].isin(TOP_COMPETITORS)]
        .groupby(["snapshot_date"], dropna=False)
        .agg(competitor_median_price=("median_price", "median"), competitor_stock_qty=("stock_qty", "sum"))
        .reset_index()
    )
    market_overall = (
        df.groupby(["snapshot_date"], dropna=False)
        .agg(market_median_price=("price_pln", "median"), market_stock_qty=("stock_qty", "sum"))
        .reset_index()
    )
    overall_pos = pirelli_overall.merge(comp_overall, on="snapshot_date", how="left").merge(
        market_overall, on="snapshot_date", how="left"
    )
    overall_pos["price_gap_vs_comp"] = overall_pos["pirelli_median_price"] - overall_pos["competitor_median_price"]
    overall_pos["pirelli_price_index"] = 100 * (overall_pos["pirelli_median_price"] / overall_pos["market_median_price"])
    overall_pos["granularity"] = "overall"
    overall_pos["analysis_fitment_key"] = "ALL"

    out = pd.concat([overall_pos, by_fitment], ignore_index=True, sort=False)
    out["rim_group"] = out["analysis_fitment_key"]
    return out


def _match_quality(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["snapshot_date"], dropna=False)
        .agg(
            rows=("product_code", "count"),
            canonical_rows=("is_canonical_match", "sum"),
            high_conf_rows=("is_high_confidence_match", "sum"),
        )
        .reset_index()
    )
    summary["canonical_match_rate"] = np.where(summary["rows"] > 0, summary["canonical_rows"] / summary["rows"], np.nan)
    summary["high_conf_match_rate"] = np.where(summary["rows"] > 0, summary["high_conf_rows"] / summary["rows"], np.nan)
    by_method = (
        df.groupby(["snapshot_date", "match_method"], dropna=False)
        .agg(rows=("product_code", "count"))
        .reset_index()
    )
    return summary.merge(by_method, on="snapshot_date", how="left")


def _keyfitment_checkpoint(df: pd.DataFrame) -> pd.DataFrame:
    scoped = df[df["is_high_confidence_match"] & df["brand"].isin(FOCUS_BRANDS)].copy()
    if scoped.empty:
        return scoped

    group_cols = ["snapshot_date", "segment_reference_group", "key_fitments", "brand", "pattern_set", "size_root"]
    agg = (
        scoped.groupby(group_cols, dropna=False)
        .agg(
            rows=("product_code", "count"),
            stock_qty=("stock_qty", "sum"),
            median_price=("price_pln", "median"),
            list_price=("list_price", "median"),
            avg_effective_discount=("effective_all_in_discount", "median"),
        )
        .reset_index()
    )
    agg["implied_discount_vs_list"] = 1 - (agg["median_price"] / agg["list_price"])
    agg = _attach_week_offsets(
        agg,
        value_col="median_price",
        keys=["segment_reference_group", "key_fitments", "brand", "pattern_set", "size_root"],
    )
    agg = _attach_week_offsets(
        agg,
        value_col="stock_qty",
        keys=["segment_reference_group", "key_fitments", "brand", "pattern_set", "size_root"],
    )
    return agg


def _target_groups(connection: sqlite3.Connection) -> list[str]:
    mapping = pd.read_sql_query(
        """
        SELECT DISTINCT segment_reference_group
        FROM ref_canonical_fitment_mapping
        WHERE segment_reference_group IS NOT NULL
          AND TRIM(segment_reference_group) <> ''
        """,
        connection,
    )
    groups = mapping["segment_reference_group"].astype("string").str.strip().dropna().tolist()
    return sorted([group for group in groups if group])


def _recap_by_brand_weighted_index(df: pd.DataFrame, connection: sqlite3.Connection) -> pd.DataFrame:
    scoped = df[
        df["is_high_confidence_match"].fillna(False)
        & df["brand"].isin(RECAP_BRANDS)
        & df["segment_reference_group"].notna()
        & (df["segment_reference_group"].astype("string").str.strip() != "")
    ].copy()
    if scoped.empty:
        return pd.DataFrame()

    target_groups = _target_groups(connection)
    if not target_groups:
        target_groups = sorted(scoped["segment_reference_group"].astype("string").str.strip().dropna().unique().tolist())

    seg_brand = (
        scoped[scoped["segment_reference_group"].isin(target_groups)]
        .groupby(["snapshot_date", "segment_reference_group", "brand"], dropna=False)
        .agg(median_price=("price_pln", "median"), stock_qty=("stock_qty", "sum"))
        .reset_index()
    )
    if seg_brand.empty:
        return pd.DataFrame()

    snapshots = seg_brand["snapshot_date"].dropna().unique().tolist()
    grid = pd.MultiIndex.from_product(
        [snapshots, target_groups, RECAP_BRANDS],
        names=["snapshot_date", "segment_reference_group", "brand"],
    ).to_frame(index=False)
    seg_brand = grid.merge(seg_brand, on=["snapshot_date", "segment_reference_group", "brand"], how="left")

    pirelli_seg = seg_brand[seg_brand["brand"] == "Pirelli"][
        ["snapshot_date", "segment_reference_group", "median_price", "stock_qty"]
    ].rename(columns={"median_price": "pirelli_seg_price", "stock_qty": "pirelli_seg_weight"})
    seg_brand = seg_brand.merge(pirelli_seg, on=["snapshot_date", "segment_reference_group"], how="left")
    seg_brand["pirelli_seg_price"] = seg_brand.groupby("snapshot_date", dropna=False)["pirelli_seg_price"].transform(
        lambda s: s.fillna(s.mean())
    )
    seg_brand["pirelli_seg_weight"] = seg_brand.groupby("snapshot_date", dropna=False)["pirelli_seg_weight"].transform(
        lambda s: s.fillna(0)
    )
    seg_brand["median_price_filled"] = seg_brand.groupby(["snapshot_date", "brand"], dropna=False)["median_price"].transform(
        lambda s: s.fillna(s.mean())
    )
    seg_brand["median_price_filled"] = seg_brand["median_price_filled"].fillna(seg_brand["pirelli_seg_price"])
    seg_brand["is_imputed_segment"] = seg_brand["median_price"].isna()

    recap = (
        seg_brand.groupby(["snapshot_date", "brand"], dropna=False)
        .agg(
            weighted_brand_price=("median_price_filled", "mean"),
            weighted_pirelli_price=("pirelli_seg_price", "mean"),
            used_weight=("pirelli_seg_weight", "sum"),
            fitments_used=("segment_reference_group", "nunique"),
            imputed_segments=("is_imputed_segment", "sum"),
        )
        .reset_index()
    )
    recap["positioning_index"] = 100 * (recap["weighted_brand_price"] / recap["weighted_pirelli_price"])
    recap["target_segment_groups"] = len(target_groups)
    recap["weight_coverage_pct"] = np.where(
        recap["target_segment_groups"] > 0, (recap["fitments_used"] - recap["imputed_segments"]) / recap["target_segment_groups"], np.nan
    )

    recap = _attach_week_offsets(recap, value_col="positioning_index", keys=["brand"])
    recap["positioning_index_round"] = recap["positioning_index"].round().astype("Int64")
    recap["vs_prev_week_round"] = recap["positioning_index_wow_delta"].round().astype("Int64")
    recap["vs_py_round"] = recap["positioning_index_yoy_delta"].round().astype("Int64")
    return recap.sort_values(["snapshot_date", "brand"])


def _recap_latest(recap: pd.DataFrame) -> pd.DataFrame:
    if recap.empty:
        return pd.DataFrame()
    work = recap.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"], errors="coerce")
    iso = work["snapshot_date"].dt.isocalendar()
    work["iso_year"] = iso["year"]
    work["iso_week"] = iso["week"]
    latest_key = work[["iso_year", "iso_week"]].dropna().drop_duplicates().sort_values(["iso_year", "iso_week"]).tail(1)
    if latest_key.empty:
        return pd.DataFrame()
    y = latest_key.iloc[0]["iso_year"]
    w = latest_key.iloc[0]["iso_week"]
    latest = work[(work["iso_year"] == y) & (work["iso_week"] == w)].copy()
    latest["brand"] = pd.Categorical(latest["brand"], categories=RECAP_BRANDS, ordered=True)
    latest = latest.sort_values("brand")
    latest["positioning_display"] = latest["positioning_index_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x)}")
    latest["vs_prev_week_display"] = latest["vs_prev_week_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x):+d}")
    latest["vs_py_display"] = latest["vs_py_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x):+d}")
    latest["week_label"] = latest["iso_year"].map(lambda yy: f"{int(yy)}") + "-W" + latest["iso_week"].map(lambda ww: f"{int(ww):02d}")
    return latest[
        [
            "snapshot_date",
            "week_label",
            "brand",
            "positioning_display",
            "vs_prev_week_display",
            "vs_py_display",
            "positioning_index",
            "positioning_index_round",
            "vs_prev_week_round",
            "vs_py_round",
            "fitments_used",
            "weight_coverage_pct",
        ]
    ]


GOLD_INSERT_COLUMNS = {
    "gold_market_weekly": ["snapshot_date", "rows", "unique_products", "unique_sellers", "stock_qty", "median_price", "mean_price", "canonical_rows", "canonical_match_rate", "built_at_utc"],
    "gold_brand_weekly": ["snapshot_date", "brand", "rows", "unique_products", "unique_sellers", "stock_qty", "median_price", "mean_price", "median_price_prev_week", "median_price_prev_year", "median_price_wow_delta", "median_price_yoy_delta", "stock_qty_prev_week", "stock_qty_prev_year", "stock_qty_wow_delta", "stock_qty_yoy_delta", "built_at_utc"],
    "gold_segment_weekly": ["snapshot_date", "analysis_fitment_key", "brand", "rows", "stock_qty", "median_price", "unique_products", "built_at_utc"],
    "gold_seller_weekly": ["snapshot_date", "seller_norm", "brand", "rows", "stock_qty", "median_price", "built_at_utc"],
    "gold_fitment_weekly": ["snapshot_date", "fitment_position", "brand", "analysis_fitment_key", "rows", "stock_qty", "median_price", "built_at_utc"],
    "gold_price_positioning_weekly": ["snapshot_date", "pirelli_median_price", "pirelli_stock_qty", "competitor_median_price", "competitor_stock_qty", "market_median_price", "market_stock_qty", "price_gap_vs_comp", "pirelli_price_index", "granularity", "analysis_fitment_key", "rim_group", "price_gap_vs_comp_prev_week", "price_gap_vs_comp_prev_year", "price_gap_vs_comp_wow_delta", "price_gap_vs_comp_yoy_delta", "pirelli_stock_qty_prev_week", "pirelli_stock_qty_prev_year", "pirelli_stock_qty_wow_delta", "pirelli_stock_qty_yoy_delta", "built_at_utc"],
    "gold_mapping_match_quality_weekly": ["snapshot_date", "rows_x", "canonical_rows", "high_conf_rows", "canonical_match_rate", "high_conf_match_rate", "match_method", "rows_y", "built_at_utc"],
    "gold_keyfitment_checkpoint_weekly": ["snapshot_date", "segment_reference_group", "key_fitments", "brand", "pattern_set", "size_root", "rows", "stock_qty", "median_price", "list_price", "avg_effective_discount", "implied_discount_vs_list", "median_price_prev_week", "median_price_prev_year", "median_price_wow_delta", "median_price_yoy_delta", "stock_qty_prev_week", "stock_qty_prev_year", "stock_qty_wow_delta", "stock_qty_yoy_delta", "built_at_utc"],
    "gold_recap_by_brand_weekly": ["snapshot_date", "brand", "weighted_brand_price", "weighted_pirelli_price", "used_weight", "fitments_used", "imputed_segments", "positioning_index", "target_segment_groups", "weight_coverage_pct", "positioning_index_prev_week", "positioning_index_prev_year", "positioning_index_wow_delta", "positioning_index_yoy_delta", "positioning_index_round", "vs_prev_week_round", "vs_py_round", "built_at_utc"],
    "gold_recap_by_brand_latest": ["snapshot_date", "week_label", "brand", "positioning_display", "vs_prev_week_display", "vs_py_display", "positioning_index", "positioning_index_round", "vs_prev_week_round", "vs_py_round", "fitments_used", "weight_coverage_pct", "built_at_utc"],
}


@dataclass(frozen=True)
class GoldBuildResult:
    db_path: Path
    rows_by_table: dict[str, int]


def _load_silver(connection: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query("SELECT * FROM silver_motorcycle_weekly", connection)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["price_pln"] = pd.to_numeric(df["price_pln"], errors="coerce")
    df["stock_qty"] = pd.to_numeric(df.get("stock_qty"), errors="coerce").fillna(0)
    for bool_col in ["is_canonical_match", "is_high_confidence_match", "is_extra_3pct_set"]:
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(0).astype(int).astype(bool)
    df["analysis_fitment_key"] = _analysis_fitment_key(df)
    return df.loc[df["price_pln"].notna()].copy()


def _delete_all_gold(connection: sqlite3.Connection) -> None:
    for table in GOLD_INSERT_COLUMNS:
        connection.execute(f"DELETE FROM {table}")


def _insert_dataframe(connection: sqlite3.Connection, table_name: str, df: pd.DataFrame, built_at_utc: str) -> int:
    if df.empty:
        return 0
    columns = GOLD_INSERT_COLUMNS[table_name]
    work = df.copy()
    work["built_at_utc"] = built_at_utc
    records = work[columns].where(work[columns].notna(), None).to_dict(orient="records")
    rows = [tuple(_sqlite_value(record[column]) for column in columns) for record in records]
    sql_columns = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})", rows)
    return len(rows)


def _sqlite_value(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d")
    if pd.isna(value):
        return None
    return value


def build_gold_marts(db_path: Path) -> GoldBuildResult:
    with connect_sqlite(db_path) as connection:
        df = _load_silver(connection)
        if df.empty:
            raise OperatorFacingError(
                "Gold marts cannot be built because the silver dataset is empty. Build silver successfully before rebuilding marts."
            )
        df_high = df[df["is_high_confidence_match"].fillna(False)].copy()

        market = (
            df.groupby(["snapshot_date"], dropna=False)
            .agg(
                rows=("product_code", "count"),
                unique_products=("product_code", "nunique"),
                unique_sellers=("seller_norm", "nunique"),
                stock_qty=("stock_qty", "sum"),
                median_price=("price_pln", "median"),
                mean_price=("price_pln", "mean"),
                canonical_rows=("is_canonical_match", "sum"),
            )
            .reset_index()
        )
        market["canonical_match_rate"] = np.where(market["rows"] > 0, market["canonical_rows"] / market["rows"], np.nan)

        brand = (
            df[df["brand"].isin(FOCUS_BRANDS)]
            .groupby(["snapshot_date", "brand"], dropna=False)
            .agg(
                rows=("product_code", "count"),
                unique_products=("product_code", "nunique"),
                unique_sellers=("seller_norm", "nunique"),
                stock_qty=("stock_qty", "sum"),
                median_price=("price_pln", "median"),
                mean_price=("price_pln", "mean"),
            )
            .reset_index()
        )
        brand = _attach_week_offsets(brand, value_col="median_price", keys=["brand"])
        brand = _attach_week_offsets(brand, value_col="stock_qty", keys=["brand"])

        segment = (
            df_high[df_high["brand"].isin(FOCUS_BRANDS)]
            .groupby(["snapshot_date", "analysis_fitment_key", "brand"], dropna=False)
            .agg(rows=("product_code", "count"), stock_qty=("stock_qty", "sum"), median_price=("price_pln", "median"), unique_products=("product_code", "nunique"))
            .reset_index()
        )

        seller = (
            df_high[df_high["brand"].isin(FOCUS_BRANDS)]
            .groupby(["snapshot_date", "seller_norm", "brand"], dropna=False)
            .agg(rows=("product_code", "count"), stock_qty=("stock_qty", "sum"), median_price=("price_pln", "median"))
            .reset_index()
        )

        fitment = (
            df_high[df_high["brand"].isin(FOCUS_BRANDS)]
            .groupby(["snapshot_date", "fitment_position", "brand", "analysis_fitment_key"], dropna=False)
            .agg(rows=("product_code", "count"), stock_qty=("stock_qty", "sum"), median_price=("price_pln", "median"))
            .reset_index()
        )

        positioning = _price_positioning(df_high[df_high["brand"].isin(["Pirelli", *TOP_COMPETITORS])].copy())
        positioning = _attach_week_offsets(positioning, value_col="price_gap_vs_comp", keys=["granularity", "analysis_fitment_key"])
        positioning = _attach_week_offsets(positioning, value_col="pirelli_stock_qty", keys=["granularity", "analysis_fitment_key"])

        match_quality = _match_quality(df)
        checkpoint = _keyfitment_checkpoint(df_high)
        recap = _recap_by_brand_weighted_index(df, connection)
        recap_latest = _recap_latest(recap)
        built_at_utc = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")

        _delete_all_gold(connection)
        rows_by_table = {
            "gold_market_weekly": _insert_dataframe(connection, "gold_market_weekly", market, built_at_utc),
            "gold_brand_weekly": _insert_dataframe(connection, "gold_brand_weekly", brand, built_at_utc),
            "gold_segment_weekly": _insert_dataframe(connection, "gold_segment_weekly", segment, built_at_utc),
            "gold_seller_weekly": _insert_dataframe(connection, "gold_seller_weekly", seller, built_at_utc),
            "gold_fitment_weekly": _insert_dataframe(connection, "gold_fitment_weekly", fitment, built_at_utc),
            "gold_price_positioning_weekly": _insert_dataframe(connection, "gold_price_positioning_weekly", positioning, built_at_utc),
            "gold_mapping_match_quality_weekly": _insert_dataframe(connection, "gold_mapping_match_quality_weekly", match_quality, built_at_utc),
            "gold_keyfitment_checkpoint_weekly": _insert_dataframe(connection, "gold_keyfitment_checkpoint_weekly", checkpoint, built_at_utc),
            "gold_recap_by_brand_weekly": _insert_dataframe(connection, "gold_recap_by_brand_weekly", recap, built_at_utc),
            "gold_recap_by_brand_latest": _insert_dataframe(connection, "gold_recap_by_brand_latest", recap_latest, built_at_utc),
        }
        connection.commit()

    return GoldBuildResult(db_path=db_path, rows_by_table=rows_by_table)
