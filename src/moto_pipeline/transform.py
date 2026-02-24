from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .canonical import assert_high_confidence_token_integrity, build_canonical_reference, match_to_canonical
from .io import read_weekly_csv, write_df
from .settings import INPUT_COLUMNS, MOTORCYCLE_TYPE, RAW_DIR, SILVER_DIR


def normalize_text(series: pd.Series) -> pd.Series:
    """Normalize text fields by trimming and collapsing whitespace.

    Args:
        series: Input text series.

    Returns:
        Normalized string series.
    """
    return (
        series.fillna("")
        .astype("string")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def rim_group(rim_value: pd.Series) -> pd.Series:
    """Bucket rim size into reporting groups.

    Args:
        rim_value: Rim diameter series.

    Returns:
        Rim group labels.
    """
    rim_num = pd.to_numeric(rim_value, errors="coerce")
    out = pd.Series("Unknown", index=rim_value.index, dtype="string")
    out = out.mask(rim_num <= 13, "<=13")
    out = out.mask((rim_num >= 14) & (rim_num <= 16), "14-16")
    out = out.mask(rim_num == 17, "17")
    out = out.mask(rim_num == 18, "18")
    out = out.mask(rim_num >= 19, "19+")
    return out


def fitment_position(name_col: pd.Series) -> pd.Series:
    """Infer FRONT/REAR fitment from product name.

    Args:
        name_col: Product name series.

    Returns:
        Fitment position labels.
    """
    name_norm = normalize_text(name_col).str.upper()
    out = pd.Series("Unknown", index=name_col.index, dtype="string")
    out = out.mask(name_norm.str.contains("FRONT", na=False), "Front")
    out = out.mask(name_norm.str.contains("REAR", na=False), "Rear")
    return out


def pattern_family(name_col: pd.Series) -> pd.Series:
    """Build a lightweight pattern-family proxy from the first tokens.

    Args:
        name_col: Product name series.

    Returns:
        Pattern-family string series.
    """
    # Lightweight product line proxy used for checkpoint-style ranking tables.
    name_norm = normalize_text(name_col).str.upper()
    tokens = name_norm.str.split(" ", expand=True).iloc[:, :3].fillna("")
    return (tokens[0] + " " + tokens[1] + " " + tokens[2]).str.strip()


def build_motorcycle_silver(
    logger: logging.Logger,
    raw_dir: Path = RAW_DIR,
    silver_dir: Path = SILVER_DIR,
) -> Path:
    """Build the motorcycle silver dataset from raw snapshots.

    Args:
        logger: Pipeline logger.
        raw_dir: Raw snapshot directory root.
        silver_dir: Silver output directory.

    Returns:
        Path to written silver dataset.
    """
    raw_files = sorted(raw_dir.glob("snapshot_date=*/source.csv"))
    if not raw_files:
        raise FileNotFoundError(f"No raw snapshots found under {raw_dir}")

    chunks: list[pd.DataFrame] = []
    for raw_file in raw_files:
        snapshot = raw_file.parent.name.replace("snapshot_date=", "")
        df = read_weekly_csv(raw_file, usecols=INPUT_COLUMNS)
        df["snapshot_date"] = snapshot
        chunks.append(df)
        logger.info("Loaded %s rows from %s", len(df), raw_file)

    full = pd.concat(chunks, ignore_index=True)
    moto = full.loc[full["type"] == MOTORCYCLE_TYPE].copy()

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

    canonical_ref, campaign_ctx = build_canonical_reference()
    moto = match_to_canonical(moto, canonical_ref)
    assert_high_confidence_token_integrity(moto)
    moto["opon_all_in_discount"] = campaign_ctx.opon_all_in_discount
    moto["effective_all_in_discount"] = moto["opon_all_in_plus_extra"].fillna(campaign_ctx.opon_all_in_discount)
    moto["expected_net_price_from_list"] = moto["list_price"] * (1 - moto["effective_all_in_discount"])
    moto["discount_vs_list_implied"] = 1 - (moto["price_pln"] / moto["list_price"])

    ordered = [
        "snapshot_date",
        "iso_year",
        "iso_week",
        "brand",
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
    silver = moto[ordered].sort_values(["snapshot_date", "brand", "seller_norm", "product_code"])
    out_path = write_df(silver, silver_dir / "motorcycle_weekly.parquet", logger)
    logger.info("Motorcycle silver dataset written to %s (%s rows)", out_path, len(silver))
    return out_path
