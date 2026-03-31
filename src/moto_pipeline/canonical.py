from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from .settings import CAMPAIGN_FILE, MAPPING_FILE, PRICE_LIST_FILE


def _first_nonempty_text(series: pd.Series) -> str:
    values = series.astype("string").fillna("").str.strip()
    nonempty = values[values != ""]
    return "" if nonempty.empty else str(nonempty.iloc[0])


def _norm_text(value: object) -> str:
    """Normalize free text for robust canonical matching.

    Args:
        value: Raw text-like value.

    Returns:
        Uppercased, punctuation-normalized token string.
    """
    text = "" if value is None else str(value)
    text = text.upper()
    # Normalize punctuation first.
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    # Make alnum compounds comparable (e.g. GPR300 <-> GPR 300).
    text = re.sub(r"(?<=[A-Z])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Z])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _norm_party_name(value: object) -> str:
    """Normalize customer or seller names for loose entity matching.

    Args:
        value: Raw party name.

    Returns:
        Uppercased token string with legal/company boilerplate removed.
    """
    text = _norm_text(value)
    if not text:
        return ""
    tokens = [t for t in text.split() if t not in {
        "SP",
        "Z",
        "OO",
        "O",
        "SA",
        "SK",
        "K",
        "AG",
        "POLSKA",
        "SPOLKA",
        "OGRANICZONA",
        "ODPOWIEDZIALNOSCIA",
        "DAWNIEJ",
    }]
    return " ".join(tokens)


def _contains_phrase(text: str, phrase: str) -> int:
    """Return 1 when phrase exists as a full-token sequence in text.

    Args:
        text: Candidate text.
        phrase: Phrase to search.

    Returns:
        1 if present, else 0.
    """
    if not text or not phrase:
        return 0
    return 1 if re.search(rf"\b{re.escape(phrase)}\b", text) else 0


def _all_tokens_present(text: str, pattern: str) -> int:
    """Return 1 when every token from pattern exists in text.

    Args:
        text: Candidate text.
        pattern: Canonical pattern text.

    Returns:
        1 if all pattern tokens are present, else 0.
    """
    text_tokens = set((text or "").split())
    pattern_tokens = [t for t in (pattern or "").split() if t]
    if not pattern_tokens:
        return 0
    return 1 if all(tok in text_tokens for tok in pattern_tokens) else 0


def extract_size_root(value: object) -> str:
    """Extract normalized size root token (e.g. ``120/70 17``).

    Args:
        value: Source size text.

    Returns:
        Extracted size root or empty string.
    """
    text = _norm_text(value).replace("ZR", "R").replace(" ZR ", " R ")
    m = re.search(r"(\d{2,3})\s*/?\s*(\d{2,3})\s*[-R]?\s*(\d{2})", text)
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)} {m.group(3)}"


def normalize_brand(value: object) -> str:
    """Normalize brand text to title case token format.

    Args:
        value: Source brand text.

    Returns:
        Normalized brand string.
    """
    text = _norm_text(value).title()
    return text


def _read_campaign_customer_allin(campaign_file: Path = CAMPAIGN_FILE) -> float:
    """Read all-in discount from campaign file.

    Args:
        campaign_file: Campaign workbook path.

    Returns:
        All-in discount ratio for Platforma Opon.
    """
    raw = pd.read_excel(campaign_file, sheet_name="rebate scheme", header=1)
    renamed = raw.rename(columns={raw.columns[0]: "customer", raw.columns[2]: "all_in_discount"})
    renamed["customer"] = renamed["customer"].astype("string")
    row = renamed[renamed["customer"].str.upper().str.contains("Platforma Opon", na=False)]
    if row.empty:
        return 0.0
    return float(pd.to_numeric(row.iloc[0]["all_in_discount"], errors="coerce") or 0.0)


def _read_campaign_pattern_extras(campaign_file: Path = CAMPAIGN_FILE) -> pd.DataFrame:
    """Read extra discount rules by canonical pattern set.

    Args:
        campaign_file: Campaign workbook path.

    Returns:
        Dataframe with normalized pattern set and extra discount.
    """
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


def load_campaign_customer_discounts(campaign_file: Path = CAMPAIGN_FILE) -> pd.DataFrame:
    """Load customer-channel discounts from the campaign workbook.

    Args:
        campaign_file: Campaign workbook path.

    Returns:
        Dataframe with one row per campaign customer channel.
    """
    raw = pd.read_excel(campaign_file, sheet_name="rebate scheme", header=1)
    top = raw.iloc[1:9, [0, 1, 2]].copy()
    top.columns = ["customer", "additional_discount_for_pattern_sets", "all_in_discount"]
    top["customer"] = top["customer"].astype("string").str.strip()
    top["customer_norm"] = top["customer"].map(_norm_party_name)
    top["additional_discount_for_pattern_sets"] = pd.to_numeric(
        top["additional_discount_for_pattern_sets"], errors="coerce"
    )
    top["all_in_discount"] = pd.to_numeric(top["all_in_discount"], errors="coerce")
    top = top[top["customer"].notna() & (top["customer"] != "")]
    return top.reset_index(drop=True)


def match_party_to_campaign_customer(
    party_name: object,
    customers: pd.DataFrame,
    min_score: float = 88.0,
) -> tuple[str | None, float]:
    """Match a seller/customer name to a campaign channel label.

    Args:
        party_name: Seller or customer name from weekly data.
        customers: Campaign customer discount table.
        min_score: Minimum fuzzy score for accepting a non-exact match.

    Returns:
        Tuple of matched customer label and confidence score.
    """
    party_norm = _norm_party_name(party_name)
    if not party_norm or customers.empty:
        return None, 0.0

    exact = customers[customers["customer_norm"].eq(party_norm)]
    if not exact.empty:
        return str(exact.iloc[0]["customer"]), 100.0

    contains = customers[
        customers["customer_norm"].astype("string").map(
            lambda c: bool(c) and (c in party_norm or party_norm in c)
        )
    ]
    if not contains.empty:
        row = contains.iloc[0]
        score = float(
            max(
                fuzz.token_set_ratio(party_norm, str(row["customer_norm"])),
                fuzz.partial_ratio(party_norm, str(row["customer_norm"])),
            )
        )
        return str(row["customer"]), score

    party_tokens = {t for t in party_norm.split() if t}
    scored = customers.copy()
    scored["shared_token_count"] = scored["customer_norm"].astype("string").map(
        lambda c: len(party_tokens & {t for t in str(c).split() if t})
    )
    scored = scored[scored["shared_token_count"] > 0].copy()
    if scored.empty:
        return None, 0.0

    scored["match_score"] = scored["customer_norm"].astype("string").map(
        lambda c: float(
            max(
                fuzz.token_set_ratio(party_norm, c),
                fuzz.token_sort_ratio(party_norm, c),
                fuzz.partial_ratio(party_norm, c),
            )
        )
    )
    best = scored.sort_values(
        ["match_score", "shared_token_count", "customer"],
        ascending=[False, False, True],
    ).iloc[0]
    if float(best["match_score"]) < min_score:
        return None, float(best["match_score"])
    return str(best["customer"]), float(best["match_score"])


def load_canonical_mapping(mapping_file: Path = MAPPING_FILE) -> pd.DataFrame:
    """Load and normalize canonical mapping table.

    Args:
        mapping_file: Canonical mapping workbook path.

    Returns:
        Canonical mapping dataframe.
    """
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
    """Load and normalize official listing price table.

    Args:
        price_list_file: Price list workbook path.

    Returns:
        Price list dataframe with normalized keys.
    """
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


def find_turnover_file(campaign_dir: Path | None = None) -> Path | None:
    """Return the most recently modified turnover workbook, when present."""
    base_dir = campaign_dir or PRICE_LIST_FILE.parent
    candidates = sorted(base_dir.glob("turnover report *.xls*"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_turnover_weights(
    turnover_file: Path | None = None,
    *,
    mapping_file: Path = MAPPING_FILE,
    price_list_file: Path = PRICE_LIST_FILE,
) -> pd.DataFrame:
    """Aggregate SQ00 turnover into static fitment weights for positioning rollups.

    The manager request is to weight the report by last month's sales taken from the
    SAP turnover export. We use ``NETVAL1`` when available because it represents the
    turnover value directly; if a file lacks that column we fall back to ``QTYBil``.
    """
    resolved_turnover_file = turnover_file or find_turnover_file(price_list_file.parent)
    empty = pd.DataFrame(columns=["analysis_fitment_key", "turnover_weight"])
    if resolved_turnover_file is None or not resolved_turnover_file.exists():
        return empty

    turnover = pd.read_excel(resolved_turnover_file)
    if turnover.empty or "Material" not in turnover.columns:
        return empty

    weight_col = "NETVAL1" if "NETVAL1" in turnover.columns else "QTYBil"
    turnover["ipcode"] = pd.to_numeric(turnover["Material"], errors="coerce").astype("Int64")
    turnover["turnover_weight"] = pd.to_numeric(turnover[weight_col], errors="coerce")
    turnover = turnover[turnover["ipcode"].notna() & turnover["turnover_weight"].notna()].copy()
    if turnover.empty:
        return empty

    mapping = load_canonical_mapping(mapping_file)
    price_list = load_price_list(price_list_file)
    fitment_ref = mapping.merge(
        price_list[["brand", "size_root", "pattern_norm", "ipcode"]],
        left_on=["brand", "size_root", "pattern_set_norm"],
        right_on=["brand", "size_root", "pattern_norm"],
        how="left",
    )
    if fitment_ref.empty:
        return empty

    fitment_ref["analysis_fitment_key"] = fitment_ref["key_fitments"].astype("string").fillna("").str.strip()
    blank_key = fitment_ref["analysis_fitment_key"] == ""
    fitment_ref.loc[blank_key, "analysis_fitment_key"] = fitment_ref.loc[blank_key, "size_root"].astype("string").fillna("").str.strip()
    fitment_ref["ipcode"] = pd.to_numeric(fitment_ref["ipcode"], errors="coerce").astype("Int64")
    fitment_ref = fitment_ref[
        fitment_ref["ipcode"].notna()
        & fitment_ref["analysis_fitment_key"].notna()
        & (fitment_ref["analysis_fitment_key"].astype("string").str.strip() != "")
        & fitment_ref["brand"].eq("Pirelli")
    ].copy()
    if fitment_ref.empty:
        return empty

    fitment_ref = (
        fitment_ref.groupby("ipcode", dropna=False, as_index=False)
        .agg(analysis_fitment_key=("analysis_fitment_key", _first_nonempty_text))
    )
    weighted = turnover.merge(fitment_ref, on="ipcode", how="left")
    weighted = weighted[weighted["analysis_fitment_key"].notna()].copy()
    weighted["analysis_fitment_key"] = weighted["analysis_fitment_key"].astype("string").str.strip()
    weighted = weighted[weighted["analysis_fitment_key"] != ""]
    if weighted.empty:
        return empty

    return (
        weighted.groupby("analysis_fitment_key", dropna=False, as_index=False)
        .agg(turnover_weight=("turnover_weight", "sum"))
        .sort_values("analysis_fitment_key")
        .reset_index(drop=True)
    )


@dataclass
class CampaignContext:
    opon_all_in_discount: float
    pattern_extras: pd.DataFrame


def load_campaign_context(campaign_file: Path = CAMPAIGN_FILE) -> CampaignContext:
    """Load campaign-level discount context.

    Args:
        campaign_file: Campaign workbook path.

    Returns:
        Campaign context dataclass.
    """
    return CampaignContext(
        opon_all_in_discount=_read_campaign_customer_allin(campaign_file),
        pattern_extras=_read_campaign_pattern_extras(campaign_file),
    )


def build_canonical_reference() -> tuple[pd.DataFrame, CampaignContext]:
    """Build canonical reference by combining mapping, price list and campaign.

    Args:
        None.

    Returns:
        Tuple of enriched canonical reference dataframe and campaign context.
    """
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
    ref["opon_all_in_discount"] = campaign.opon_all_in_discount
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
    return ref, campaign




def assert_high_confidence_token_integrity(df: pd.DataFrame, sample_size: int = 12) -> None:
    """Validate that high-confidence matches satisfy full-token agreement.

    Args:
        df: Matched dataset to validate.
        sample_size: Number of failing sample rows to include in error text.

    Returns:
        None. Raises ``ValueError`` on integrity violations.
    """
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

def match_to_canonical(opon_df: pd.DataFrame, canonical_ref: pd.DataFrame) -> pd.DataFrame:
    """Match Platforma Opon rows to canonical patterns using strict token-safe scoring.

    Args:
        opon_df: Input dataset from raw/silver transform step.
        canonical_ref: Enriched canonical reference dataframe.

    Returns:
        Matched dataframe with canonical fields and match diagnostics.
    """
    if opon_df.empty:
        return opon_df.copy()

    df = opon_df.copy()
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
        "opon_all_in_plus_extra",
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
