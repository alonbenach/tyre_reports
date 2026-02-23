from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from .settings import CAMPAIGN_FILE, MAPPING_FILE, PRICE_LIST_FILE


def _norm_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _contains_phrase(text: str, phrase: str) -> int:
    if not text or not phrase:
        return 0
    return 1 if re.search(rf"\b{re.escape(phrase)}\b", text) else 0


def _all_tokens_present(text: str, pattern: str) -> int:
    text_tokens = set((text or "").split())
    pattern_tokens = [t for t in (pattern or "").split() if t]
    if not pattern_tokens:
        return 0
    return 1 if all(tok in text_tokens for tok in pattern_tokens) else 0


def extract_size_root(value: object) -> str:
    text = _norm_text(value).replace("ZR", "R").replace(" ZR ", " R ")
    m = re.search(r"(\d{2,3})\s*/?\s*(\d{2,3})\s*[-R]?\s*(\d{2})", text)
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)} {m.group(3)}"


def normalize_brand(value: object) -> str:
    text = _norm_text(value).title()
    return text


def _read_campaign_customer_allin(campaign_file: Path = CAMPAIGN_FILE) -> float:
    raw = pd.read_excel(campaign_file, sheet_name="rebate scheme", header=1)
    renamed = raw.rename(columns={raw.columns[0]: "customer", raw.columns[2]: "all_in_discount"})
    renamed["customer"] = renamed["customer"].astype("string")
    row = renamed[renamed["customer"].str.upper().str.contains("OPONEO", na=False)]
    if row.empty:
        return 0.0
    return float(pd.to_numeric(row.iloc[0]["all_in_discount"], errors="coerce") or 0.0)


def _read_campaign_pattern_extras(campaign_file: Path = CAMPAIGN_FILE) -> pd.DataFrame:
    raw = pd.read_excel(campaign_file, sheet_name="rebate scheme", header=1)
    c0, c1, c2 = raw.columns[:3]
    marker = raw[c0].astype("string").str.upper().str.contains("ADDITIONAL DISCOUNT FOR PATTERN SETS", na=False)
    if not marker.any():
        return pd.DataFrame(columns=["pattern_set_norm", "extra_discount"])

    start_idx = marker[marker].index[0] + 1
    section = raw.loc[start_idx:, [c0, c2]].copy()
    section = section.rename(columns={c0: "pattern_set", c2: "extra_discount"})
    section["extra_discount"] = pd.to_numeric(section["extra_discount"], errors="coerce")
    section = section[section["extra_discount"].notna()]
    section["pattern_set_norm"] = section["pattern_set"].map(_norm_text)
    section = section[section["pattern_set_norm"] != ""]
    return section[["pattern_set_norm", "extra_discount"]].drop_duplicates()


def load_canonical_mapping(mapping_file: Path = MAPPING_FILE) -> pd.DataFrame:
    mapping = pd.read_excel(mapping_file, sheet_name="mapping")
    mapping = mapping.rename(
        columns={
            "Pattern Set": "pattern_set",
            "Brand": "brand",
            "Segment Reference Group": "segment_reference_group",
            "key fitments": "key_fitments",
            "size": "size_text",
        }
    )
    mapping["brand"] = mapping["brand"].map(normalize_brand)
    mapping["pattern_set_norm"] = mapping["pattern_set"].map(_norm_text)
    mapping["size_root"] = mapping["size_text"].map(extract_size_root)
    mapping["segment_reference_group"] = mapping["segment_reference_group"].astype("string").fillna("")
    mapping["key_fitments"] = mapping["key_fitments"].astype("string").fillna("")
    return mapping[
        ["brand", "pattern_set", "pattern_set_norm", "segment_reference_group", "key_fitments", "size_text", "size_root"]
    ].drop_duplicates()


def load_price_list(price_list_file: Path = PRICE_LIST_FILE) -> pd.DataFrame:
    pl = pd.read_excel(price_list_file, sheet_name="listing price")
    pl = pl.rename(
        columns={
            "Marka": "brand_raw",
            "BIEŻNIK": "pattern_name",
            "price list": "list_price",
            "size": "size_text",
            "Segment Reference Group": "segment_reference_group",
            "Ipcode": "ipcode",
        }
    )
    pl["brand"] = pl["brand_raw"].map(normalize_brand)
    pl["pattern_norm"] = pl["pattern_name"].map(_norm_text)
    pl["size_root"] = pl["size_text"].map(extract_size_root)
    pl["list_price"] = pd.to_numeric(pl["list_price"], errors="coerce")
    return pl[
        ["brand", "pattern_name", "pattern_norm", "size_text", "size_root", "segment_reference_group", "list_price", "ipcode"]
    ]


@dataclass
class CampaignContext:
    oponeo_all_in_discount: float
    pattern_extras: pd.DataFrame


def load_campaign_context(campaign_file: Path = CAMPAIGN_FILE) -> CampaignContext:
    return CampaignContext(
        oponeo_all_in_discount=_read_campaign_customer_allin(campaign_file),
        pattern_extras=_read_campaign_pattern_extras(campaign_file),
    )


def build_canonical_reference() -> tuple[pd.DataFrame, CampaignContext]:
    mapping = load_canonical_mapping()
    price_list = load_price_list()
    campaign = load_campaign_context()

    ref = mapping.merge(
        price_list[["brand", "size_root", "pattern_norm", "list_price", "ipcode"]],
        left_on=["brand", "size_root", "pattern_set_norm"],
        right_on=["brand", "size_root", "pattern_norm"],
        how="left",
    )
    ref = ref.drop(columns=["pattern_norm"])
    ref = ref.merge(campaign.pattern_extras, on="pattern_set_norm", how="left")
    ref["extra_discount"] = pd.to_numeric(ref["extra_discount"], errors="coerce").fillna(0.0)
    ref["is_extra_3pct_set"] = ref["extra_discount"] >= 0.03 - 1e-9
    ref["oponeo_all_in_discount"] = campaign.oponeo_all_in_discount
    ref["oponeo_all_in_plus_extra"] = ref["oponeo_all_in_discount"] + ref["extra_discount"]
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
            oponeo_all_in_discount=("oponeo_all_in_discount", "max"),
            oponeo_all_in_plus_extra=("oponeo_all_in_plus_extra", "max"),
        )
    )
    return ref, campaign




def assert_high_confidence_token_integrity(df: pd.DataFrame, sample_size: int = 12) -> None:
    if df.empty:
        return
    required = {"is_high_confidence_match", "pattern_set", "name_norm"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for token integrity check: {missing}")

    work = df[df["is_high_confidence_match"].fillna(False)].copy()
    if work.empty:
        return

    work["_pattern_norm"] = work["pattern_set"].map(_norm_text)
    work["_name_norm"] = work["name_norm"].map(_norm_text)
    if "pattern_family" in work.columns:
        work["_family_norm"] = work["pattern_family"].map(_norm_text)
    else:
        work["_family_norm"] = ""

    work["_token_ok"] = work.apply(
        lambda r: (
            _all_tokens_present(str(r["_name_norm"]), str(r["_pattern_norm"]))
            or _all_tokens_present(str(r["_family_norm"]), str(r["_pattern_norm"]))
        ),
        axis=1,
    )
    work["_token_ok"] = work["_token_ok"].astype(bool)
    bad = work[work["_token_ok"].eq(False)].copy()
    if bad.empty:
        return

    cols = [c for c in ["snapshot_date", "brand", "product_code", "size_norm", "name_norm", "pattern_set", "match_method"] if c in bad.columns]
    sample = bad[cols].head(sample_size).to_dict("records")
    raise ValueError(
        "High-confidence canonical matches failed full-token integrity check. "
        f"rows={len(bad)} sample={sample}"
    )

def match_to_canonical(oponeo_df: pd.DataFrame, canonical_ref: pd.DataFrame) -> pd.DataFrame:
    if oponeo_df.empty:
        return oponeo_df.copy()

    df = oponeo_df.copy()
    ref = canonical_ref.copy()

    df["brand"] = df["brand"].map(normalize_brand)
    df["size_root"] = df["size_norm"].map(extract_size_root)
    df["name_norm_ref"] = df["name_norm"].map(_norm_text)
    df["pattern_family_norm"] = df["pattern_family"].map(_norm_text) if "pattern_family" in df.columns else ""

    ref_key = ref.sort_values(["brand", "size_root"]).copy()
    ref_key["match_rank"] = ref_key.groupby(["brand", "size_root"]).cumcount() + 1

    merged = df.merge(
        ref_key,
        on=["brand", "size_root"],
        how="left",
        suffixes=("", "_ref"),
    )

    has_candidate = merged["pattern_set"].notna()
    merged["pattern_match_score"] = 0.0
    merged["pattern_phrase_hit"] = 0
    merged["pattern_all_tokens_hit"] = 0
    merged["pattern_token_count"] = merged["pattern_set_norm"].astype("string").fillna("").str.split().str.len()
    merged["pattern_char_len"] = merged["pattern_set_norm"].astype("string").fillna("").str.len()
    merged.loc[has_candidate, "pattern_match_score"] = merged.loc[has_candidate].apply(
        lambda r: float(
            max(
                fuzz.token_set_ratio(r["name_norm_ref"], r["pattern_set_norm"]),
                fuzz.token_sort_ratio(r["name_norm_ref"], r["pattern_set_norm"]),
                fuzz.token_set_ratio(str(r.get("pattern_family_norm", "")), r["pattern_set_norm"]),
                fuzz.partial_ratio(r["name_norm_ref"], r["pattern_set_norm"]),
            )
        ),
        axis=1,
    )
    merged.loc[has_candidate, "pattern_phrase_hit"] = merged.loc[has_candidate].apply(
        lambda r: _contains_phrase(str(r["name_norm_ref"]), str(r["pattern_set_norm"])),
        axis=1,
    )
    merged.loc[has_candidate, "pattern_all_tokens_hit"] = merged.loc[has_candidate].apply(
        lambda r: max(
            _all_tokens_present(str(r["name_norm_ref"]), str(r["pattern_set_norm"])),
            _all_tokens_present(str(r.get("pattern_family_norm", "")), str(r["pattern_set_norm"])),
        ),
        axis=1,
    )

    merged = merged.sort_values(
        [
            "snapshot_date",
            "brand",
            "seller_norm",
            "product_code",
            "pattern_all_tokens_hit",
            "pattern_match_score",
            "pattern_phrase_hit",
            "pattern_token_count",
            "pattern_char_len",
            "match_rank",
        ],
        ascending=[True, True, True, True, False, False, False, False, False, True],
    )
    best = merged.drop_duplicates(
        subset=["snapshot_date", "brand", "seller_norm", "product_code", "price_pln", "size_root"], keep="first"
    ).copy()

    strict_candidate = best["pattern_set"].notna() & best["pattern_all_tokens_hit"].eq(1)
    best["match_method"] = "unmatched"
    best.loc[strict_candidate & (best["pattern_match_score"] >= 95), "match_method"] = "brand_size_exact_pattern"
    best.loc[
        strict_candidate & best["match_method"].eq("unmatched") & (best["pattern_match_score"] >= 70),
        "match_method",
    ] = "brand_size_fuzzy_pattern"
    best.loc[strict_candidate & best["match_method"].eq("unmatched"), "match_method"] = "brand_size_low_score"

    high_conf_mask = best["match_method"].isin(["brand_size_exact_pattern", "brand_size_fuzzy_pattern"])
    non_high_conf_mask = ~high_conf_mask

    # Enforce canonical agreement on both pattern and fitment root.
    # Low-score rows keep diagnostics, but lose canonical attributes so they cannot drive segment analytics.
    canonical_cols = [
        "pattern_set",
        "pattern_set_norm",
        "segment_reference_group",
        "key_fitments",
        "size_text",
        "list_price",
        "ipcode",
        "extra_discount",
        "is_extra_3pct_set",
        "oponeo_all_in_plus_extra",
    ]
    for col in canonical_cols:
        if col in best.columns:
            if pd.api.types.is_bool_dtype(best[col].dtype):
                best.loc[non_high_conf_mask, col] = False
            elif pd.api.types.is_numeric_dtype(best[col].dtype):
                best.loc[non_high_conf_mask, col] = np.nan
            else:
                best.loc[non_high_conf_mask, col] = pd.NA

    best["is_canonical_match"] = high_conf_mask
    best["is_high_confidence_match"] = high_conf_mask
    return best.drop(columns=["name_norm_ref", "pattern_family_norm"])
