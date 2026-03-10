from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from .canonical import (
    load_campaign_customer_discounts,
    load_canonical_mapping,
    match_party_to_campaign_customer,
)
from .settings import GOLD_DIR, REPORT_DIR, SILVER_DIR

OFFEROR_BRANDS = ["Pirelli", "Metzeler"]
GROUP_ORDER = [
    "706 - SUPERSPORT 1st",
    "706 - SUPERSPORT 2nd",
    "706 - SUPERSPORT 3rd",
    "751 - SPORT TOURING RADIAL 1st",
    "751 - SPORT TOURING RADIAL 2nd",
    "751 - SPORT TOURING RADIAL 3rd",
    "707 - RACING STREET 1st",
    "746 - ENDURO STREET 1st",
    "747 - ENDURO ON/OFF 1st",
    "762 - CUSTOM / TOURING X-PLY 1st",
]


def _read_gold(gold_dir: Path, filename: str) -> pd.DataFrame:
    """Read one mart file from gold layer.

    Args:
        gold_dir: Gold directory path.
        filename: Gold CSV file name.

    Returns:
        Loaded dataframe.
    """
    path = gold_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Expected mart file not found: {path}")
    return pd.read_csv(path)


def _read_silver(silver_dir: Path) -> pd.DataFrame:
    """Read silver dataset from parquet with CSV fallback.

    Args:
        silver_dir: Silver directory path.

    Returns:
        Loaded silver dataframe.
    """
    parquet = silver_dir / "motorcycle_weekly.parquet"
    csv_file = silver_dir / "motorcycle_weekly.csv"
    if parquet.exists():
        try:
            return pd.read_parquet(parquet)
        except Exception:
            pass
    if csv_file.exists():
        return pd.read_csv(csv_file, low_memory=False)
    raise FileNotFoundError(
        "No silver dataset found (expected motorcycle_weekly.parquet or .csv)."
    )


def _safe_prev_date(
    series: pd.Series,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Return latest and previous timestamps from a date series.

    Args:
        series: Date-like series.

    Returns:
        Tuple of ``(latest, previous)`` timestamps.
    """
    values = sorted(pd.to_datetime(series, errors="coerce").dropna().unique())
    if not values:
        return None, None
    latest = pd.Timestamp(values[-1])
    prev = pd.Timestamp(values[-2]) if len(values) > 1 else None
    return latest, prev


def _week_label(date: pd.Timestamp) -> str:
    """Format ISO week label.

    Args:
        date: Snapshot timestamp.

    Returns:
        Week label in ``Wxx`` format.
    """
    return f"W{int(date.isocalendar().week):02d}"


def _parse_group_label(segment_reference_group: str) -> tuple[str, str]:
    """Split canonical group text into segment and line labels.

    Args:
        segment_reference_group: Canonical group label.

    Returns:
        Tuple of ``(segment, line)``.
    """
    text = str(segment_reference_group or "").strip()
    m = re.match(r"^\s*\d+\s*-\s*(.+?)\s+(1st|2nd|3rd)\s*$", text, flags=re.I)
    if not m:
        return text, "-"
    segment = m.group(1).strip()
    line = m.group(2).strip().title()
    return segment, line


def _fitment_roots(key_fitments: str) -> list[str]:
    """Extract normalized fitment roots from key fitments text.

    Args:
        key_fitments: Canonical key fitments text with ``&`` separator.

    Returns:
        List of ``width/profile rim`` roots.
    """
    chunks = [c.strip() for c in str(key_fitments or "").split("&") if c.strip()]
    roots: list[str] = []
    for chunk in chunks:
        m = re.search(r"(\d{2,3}\s*/\s*\d{2,3}\s+\d{2})", chunk)
        if m:
            root = re.sub(r"\s+", " ", m.group(1).replace(" /", "/").replace("/ ", "/"))
            roots.append(root)
    return roots


def _mode_or_dash(series: pd.Series) -> str:
    """Return modal non-empty value or dash.

    Args:
        series: Input series.

    Returns:
        Mode value or ``-``.
    """
    vals = series.dropna().astype("string").str.strip()
    vals = vals[vals != ""]
    if vals.empty:
        return "-"
    return str(vals.mode().iloc[0])


def _safe_float(value: object) -> float:
    """Convert scalar to float with NaN fallback.

    Args:
        value: Numeric-like scalar value.

    Returns:
        Parsed float or ``np.nan``.
    """
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return float(out) if pd.notna(out) else np.nan
    except Exception:
        return np.nan


def _campaign_discount_pct(
    seller_name: object,
    is_extra_set: bool,
    customers: pd.DataFrame,
) -> tuple[float, str]:
    """Resolve seller-specific campaign discount percentage.

    Args:
        seller_name: Seller name from weekly data.
        is_extra_set: Whether the canonical pattern belongs to the extra-discount set path.
        customers: Campaign customer discount table.

    Returns:
        Tuple of resolved discount ratio and matched/fallback channel label.
    """
    discount_col = (
        "additional_discount_for_pattern_sets" if is_extra_set else "all_in_discount"
    )
    matched_customer, _score = match_party_to_campaign_customer(seller_name, customers)
    if matched_customer is not None:
        matched = customers[customers["customer"].eq(matched_customer)]
        if not matched.empty:
            resolved = _safe_float(matched.iloc[0][discount_col])
            if pd.notna(resolved):
                return resolved, matched_customer

    fallback = _safe_float(customers[discount_col].max()) if not customers.empty else np.nan
    return fallback, "MAX_CHANNEL_FALLBACK"


def _clean_code(value: object) -> str:
    """Normalize code-like identifiers as text.

    Args:
        value: Raw code value.

    Returns:
        Text code without numeric suffix artifacts (for example ``.0``).
    """
    text = str(value).strip()
    if text in {"", "-", "<NA>", "nan", "NaN", "None"}:
        return "-"
    num = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(num) and float(num).is_integer():
        return str(int(num))
    return text


def _build_page1_table(
    silver: pd.DataFrame,
    latest: pd.Timestamp,
    prev: pd.Timestamp | None,
) -> pd.DataFrame:
    """Build page-1 offeror table from canonical-matched rows.

    Args:
        silver: Silver dataframe.
        latest: Latest snapshot date.
        prev: Previous snapshot date.

    Returns:
        Page-1 table dataframe.
    """
    mapping = load_canonical_mapping()
    customer_discounts = load_campaign_customer_discounts()
    mapping = mapping[
        mapping["segment_reference_group"].astype("string").isin(GROUP_ORDER)
        & mapping["brand"].astype("string").isin(OFFEROR_BRANDS)
    ].copy()
    if mapping.empty:
        return pd.DataFrame()

    mapping_key = (
        mapping.groupby(["segment_reference_group", "brand"], as_index=False)
        .agg(key_fitments=("key_fitments", lambda s: _mode_or_dash(pd.Series(s))))
        .copy()
    )

    work = silver.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"], errors="coerce")
    work["price_pln"] = pd.to_numeric(work["price_pln"], errors="coerce")
    work["stock_qty"] = pd.to_numeric(work["stock_qty"], errors="coerce").fillna(0)
    work["discount_vs_list_implied"] = pd.to_numeric(
        work["discount_vs_list_implied"], errors="coerce"
    )
    work["expected_net_price_from_list"] = pd.to_numeric(
        work["expected_net_price_from_list"], errors="coerce"
    )
    work = work[
        work["brand"].astype("string").isin(OFFEROR_BRANDS)
        & work["is_high_confidence_match"].fillna(False)
    ].copy()
    if work.empty:
        return pd.DataFrame()

    latest_df = work[work["snapshot_date"] == latest].copy()
    prev_df = work[work["snapshot_date"] == prev].copy() if prev is not None else pd.DataFrame()

    rows: list[dict[str, object]] = []
    for row in mapping_key.itertuples(index=False):
        group = str(row.segment_reference_group)
        brand = str(row.brand)
        key_fitments = str(row.key_fitments)
        roots = _fitment_roots(key_fitments)

        latest_group = latest_df[
            (latest_df["segment_reference_group"] == group) & (latest_df["brand"] == brand)
        ].copy()
        if latest_group.empty:
            if roots:
                for root in roots:
                    segment, line = _parse_group_label(group)
                    rows.append(
                        {
                            "segment_reference_group": group,
                            "segment": segment,
                            "line": line,
                            "key_fitments": key_fitments or "-",
                            "brand": brand,
                            "pattern_set": "-",
                            "size_root": root,
                            "size_norm": "-",
                            "ipcode": "-",
                            "first_player": "-",
                            "stock": np.nan,
                            "platforma_price": np.nan,
                            "disc_pct": np.nan,
                            "price_var_vs_lw_pct": np.nan,
                            "markup_pct": np.nan,
                            "markup_var_vs_lw_pp": np.nan,
                        }
                    )
            continue

        if roots:
            latest_for_pattern = latest_group[latest_group["size_root"].isin(roots)].copy()
        else:
            latest_for_pattern = latest_group
        if latest_for_pattern.empty:
            latest_for_pattern = latest_group

        pattern_rank = (
            latest_for_pattern.groupby("pattern_set", dropna=False)["stock_qty"]
            .sum()
            .reset_index()
            .sort_values("stock_qty", ascending=False)
        )
        if pattern_rank.empty:
            continue
        best_pattern = str(pattern_rank.iloc[0]["pattern_set"])
        latest_pattern = latest_group[latest_group["pattern_set"] == best_pattern].copy()

        target_roots = roots if roots else sorted(latest_pattern["size_root"].dropna().unique().tolist())[:2]
        if not target_roots:
            target_roots = ["-"]

        segment, line = _parse_group_label(group)
        for root in target_roots:
            latest_size = latest_pattern[latest_pattern["size_root"] == root].copy()
            if latest_size.empty:
                rows.append(
                    {
                        "segment_reference_group": group,
                        "segment": segment,
                        "line": line,
                        "key_fitments": key_fitments or "-",
                        "brand": brand,
                        "pattern_set": best_pattern,
                        "size_root": root,
                        "size_norm": "-",
                        "ipcode": "-",
                        "first_player": "-",
                        "stock": np.nan,
                        "platforma_price": np.nan,
                        "disc_pct": np.nan,
                        "price_var_vs_lw_pct": np.nan,
                        "markup_pct": np.nan,
                        "markup_var_vs_lw_pp": np.nan,
                    }
                )
                continue

            seller_rank = (
                latest_size.groupby("seller_norm", dropna=False)["stock_qty"]
                .sum()
                .reset_index()
                .sort_values("stock_qty", ascending=False)
            )
            first_player = _mode_or_dash(seller_rank.head(1)["seller_norm"])
            latest_seller = latest_size[latest_size["seller_norm"] == first_player].copy()
            stock = float(latest_seller["stock_qty"].sum())
            platforma_price = _safe_float(latest_seller["price_pln"].median())
            list_price = _safe_float(latest_seller["list_price"].median())
            is_extra_set = bool(latest_seller["is_extra_3pct_set"].fillna(False).any())
            campaign_discount_ratio, _campaign_customer = _campaign_discount_pct(
                seller_name=first_player,
                is_extra_set=is_extra_set,
                customers=customer_discounts,
            )
            campaign_net_price = (
                float(list_price * (1.0 - campaign_discount_ratio))
                if pd.notna(list_price) and list_price != 0 and pd.notna(campaign_discount_ratio)
                else np.nan
            )
            disc_pct = (
                float((1.0 - (platforma_price / list_price)) * 100.0)
                if pd.notna(list_price) and list_price != 0 and pd.notna(platforma_price)
                else np.nan
            )
            markup_pct = (
                float((platforma_price / campaign_net_price - 1.0) * 100.0)
                if pd.notna(platforma_price)
                and pd.notna(campaign_net_price)
                and campaign_net_price != 0
                else np.nan
            )
            ipcode = _clean_code(_mode_or_dash(latest_seller["ipcode"]))
            size_norm = _mode_or_dash(latest_seller["size_norm"])

            price_var_vs_lw_pct = np.nan
            markup_var_vs_lw_pp = np.nan
            if not prev_df.empty:
                prev_size = prev_df[
                    (prev_df["segment_reference_group"] == group)
                    & (prev_df["brand"] == brand)
                    & (prev_df["pattern_set"] == best_pattern)
                    & (prev_df["size_root"] == root)
                    & (prev_df["seller_norm"] == first_player)
                ].copy()
                if not prev_size.empty:
                    prev_price = _safe_float(prev_size["price_pln"].median())
                    if pd.notna(prev_price) and prev_price != 0 and pd.notna(platforma_price):
                        price_var_vs_lw_pct = float((platforma_price / prev_price - 1.0) * 100.0)
                    prev_list_price = _safe_float(prev_size["list_price"].median())
                    prev_is_extra_set = bool(prev_size["is_extra_3pct_set"].fillna(False).any())
                    prev_campaign_discount_ratio, _prev_campaign_customer = _campaign_discount_pct(
                        seller_name=first_player,
                        is_extra_set=prev_is_extra_set,
                        customers=customer_discounts,
                    )
                    prev_campaign_net_price = (
                        float(prev_list_price * (1.0 - prev_campaign_discount_ratio))
                        if pd.notna(prev_list_price)
                        and prev_list_price != 0
                        and pd.notna(prev_campaign_discount_ratio)
                        else np.nan
                    )
                    prev_markup_pct = (
                        float((prev_price / prev_campaign_net_price - 1.0) * 100.0)
                        if pd.notna(prev_price)
                        and pd.notna(prev_campaign_net_price)
                        and prev_campaign_net_price != 0
                        else np.nan
                    )
                    if pd.notna(markup_pct) and pd.notna(prev_markup_pct):
                        markup_var_vs_lw_pp = float(markup_pct - prev_markup_pct)

            rows.append(
                {
                    "segment_reference_group": group,
                    "segment": segment,
                    "line": line,
                    "key_fitments": key_fitments or "-",
                    "brand": brand,
                    "pattern_set": best_pattern,
                    "size_root": root,
                    "size_norm": size_norm,
                    "ipcode": ipcode,
                    "first_player": first_player,
                    "stock": stock,
                    "platforma_price": platforma_price,
                    "disc_pct": disc_pct,
                    "price_var_vs_lw_pct": price_var_vs_lw_pct,
                    "markup_pct": markup_pct,
                    "markup_var_vs_lw_pp": markup_var_vs_lw_pp,
                }
            )

    table = pd.DataFrame(rows)
    if table.empty:
        return table

    table["group_order"] = table["segment_reference_group"].map(
        {g: i for i, g in enumerate(GROUP_ORDER)}
    )
    table["brand_order"] = table["brand"].map({b: i for i, b in enumerate(OFFEROR_BRANDS)})
    table = table.sort_values(["group_order", "brand_order", "size_root"]).drop(
        columns=["group_order", "brand_order"]
    )
    return table


def _fmt_num(value: object, ndigits: int = 1) -> str:
    """Format numeric output in report style.

    Args:
        value: Numeric-like value.
        ndigits: Decimal precision.

    Returns:
        Formatted string or dash.
    """
    if pd.isna(value):
        return "-"
    return f"{float(value):.{ndigits}f}".replace(".", ",")


def _fmt_pct(value: object, ndigits: int = 1, signed: bool = False) -> str:
    """Format percentage values.

    Args:
        value: Numeric-like value in percentage points.
        ndigits: Decimal precision.
        signed: Show explicit sign for non-negative values.

    Returns:
        Formatted percentage string or dash.
    """
    if pd.isna(value):
        return "-"
    sign = "+" if signed and float(value) >= 0 else ""
    return f"{sign}{float(value):.{ndigits}f}%".replace(".", ",")


def _collapse_repeated_rows(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Blank repeated values to simulate merged grouped rows.

    Args:
        df: Display dataframe.
        cols: Ordered grouping columns from highest to lowest level.

    Returns:
        Dataframe with repeated group labels collapsed to empty strings.
    """
    out = df.copy()
    if out.empty:
        return out

    prev: dict[str, str] = {c: "" for c in cols}
    for idx, row in out.iterrows():
        same_prefix = True
        for col in cols:
            value = str(row.get(col, ""))
            if same_prefix and value == prev[col]:
                out.at[idx, col] = ""
            else:
                same_prefix = False
            prev[col] = value
    return out


def _group_spans(values: pd.Series) -> list[tuple[int, int]]:
    """Return inclusive row spans for contiguous identical values."""
    if values.empty:
        return []

    spans: list[tuple[int, int]] = []
    start = 0
    while start < len(values):
        current = str(values.iloc[start])
        end = start
        while end + 1 < len(values) and str(values.iloc[end + 1]) == current:
            end += 1
        spans.append((start, end))
        start = end + 1
    return spans


def _draw_pdf_table(
    ax,
    display: pd.DataFrame,
    source: pd.DataFrame,
    bbox: list[float],
    col_widths: list[float],
) -> None:
    """Draw a document-style table with merged key-fitment cells."""
    from matplotlib.patches import Rectangle

    left, bottom, width, height = bbox
    nrows = len(display)
    ncols = len(display.columns)
    widths = np.array(col_widths, dtype=float)
    widths = widths / widths.sum()
    x_edges = [left]
    for frac in widths:
        x_edges.append(x_edges[-1] + width * float(frac))

    header_h = min(0.05, height * 0.09)
    body_h = height - header_h
    row_h = body_h / max(nrows, 1)

    def draw_cell(x0: float, y0: float, w: float, h: float, face: str, edge: str, lw: float) -> None:
        ax.add_patch(
            Rectangle(
                (x0, y0),
                w,
                h,
                facecolor=face,
                edgecolor=edge,
                linewidth=lw,
                transform=ax.transAxes,
                clip_on=False,
            )
        )

    def draw_text(text: str, x0: float, y0: float, w: float, h: float, align: str = "center", bold: bool = False) -> None:
        if text == "":
            return
        if align == "left":
            x = x0 + 0.004
            ha = "left"
        elif align == "right":
            x = x0 + w - 0.004
            ha = "right"
        else:
            x = x0 + (w / 2.0)
            ha = "center"
        ax.text(
            x,
            y0 + (h / 2.0),
            text,
            ha=ha,
            va="center",
            fontsize=7.0,
            fontweight="bold" if bold else "normal",
            color="#111827",
            transform=ax.transAxes,
            clip_on=True,
        )

    for col_idx, column in enumerate(display.columns):
        x0 = x_edges[col_idx]
        w = x_edges[col_idx + 1] - x0
        y0 = bottom + body_h
        draw_cell(x0, y0, w, header_h, "#E5E7EB", "#D1D5DB", 0.8)
        draw_text(str(column), x0, y0, w, header_h, align="center", bold=True)

    left_align_cols = {"Segment Ref Group", "Key Fitments", "Brand", "Pattern Set", "Size", "First Player"}
    right_align_cols = {"Stock", "Price", "Disc %", "vs LW", "Mark-Up", "Mark-Up vs LW (pp)"}
    key_fitments_idx = list(display.columns).index("Key Fitments")
    group_spans = _group_spans(source["Segment Ref Group"])
    group_start_rows = {start for start, _end in group_spans if start > 0}

    for row_idx in range(nrows):
        y0 = bottom + body_h - ((row_idx + 1) * row_h)
        fill = "#FFFFFF" if (row_idx + 1) % 2 else "#F9FAFB"
        border_lw = 1.25 if row_idx in group_start_rows else 0.6
        border_color = "#9CA3AF" if row_idx in group_start_rows else "#D1D5DB"
        for col_idx, column in enumerate(display.columns):
            if col_idx == key_fitments_idx:
                continue
            x0 = x_edges[col_idx]
            w = x_edges[col_idx + 1] - x0
            draw_cell(x0, y0, w, row_h, fill, border_color, border_lw)
            text = str(display.iloc[row_idx, col_idx])
            if text == "nan":
                text = ""
            align = "center"
            if column in left_align_cols:
                align = "left"
            elif column in right_align_cols:
                align = "right"
            draw_text(text, x0, y0, w, row_h, align=align)

    for start, end in group_spans:
        top_y = bottom + body_h - (start * row_h)
        y0 = bottom + body_h - ((end + 1) * row_h)
        h = top_y - y0
        fill = "#FFFFFF" if (start + 1) % 2 else "#F9FAFB"
        border_lw = 1.25 if start > 0 else 0.6
        border_color = "#9CA3AF" if start > 0 else "#D1D5DB"
        x0 = x_edges[key_fitments_idx]
        w = x_edges[key_fitments_idx + 1] - x0
        draw_cell(x0, y0, w, h, fill, border_color, border_lw)
        label = str(source.iloc[start]["Key Fitments"])
        draw_text(label, x0, y0, w, h, align="left")


def build_excel_report(
    logger: logging.Logger,
    gold_dir: Path = GOLD_DIR,
    report_dir: Path = REPORT_DIR,
    silver_dir: Path = SILVER_DIR,
) -> Path:
    """Generate Offeror Focus Excel report (page 1 data only for now).

    Args:
        logger: Pipeline logger.
        gold_dir: Gold input directory.
        report_dir: Output directory.
        silver_dir: Silver input directory.

    Returns:
        Path to generated workbook.
    """
    report_dir.mkdir(parents=True, exist_ok=True)
    market = _read_gold(gold_dir, "gold_market_weekly.csv")
    market["snapshot_date"] = pd.to_datetime(market["snapshot_date"], errors="coerce")
    latest, prev = _safe_prev_date(market["snapshot_date"])
    if latest is None:
        raise ValueError("Unable to determine latest snapshot date.")

    silver = _read_silver(silver_dir)
    table = _build_page1_table(silver=silver, latest=latest, prev=prev)
    week = _week_label(latest)
    output = report_dir / f"offeror_focus_{week}_Poland.xlsx"

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        table.to_excel(writer, index=False, sheet_name="Page1_Top_Key_Fitments")

    logger.info("Offeror Focus Excel written: %s", output)
    return output


def build_pdf_report(
    logger: logging.Logger,
    gold_dir: Path = GOLD_DIR,
    report_dir: Path = REPORT_DIR,
    silver_dir: Path = SILVER_DIR,
) -> Path | None:
    """Generate Offeror Focus PDF report (page 1 only).

    Args:
        logger: Pipeline logger.
        gold_dir: Gold input directory.
        report_dir: Output directory.
        silver_dir: Silver input directory.

    Returns:
        Path to generated PDF, or ``None`` if matplotlib is unavailable.
    """
    try:
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
    except Exception as exc:  # pragma: no cover
        logger.warning("Skipping Offeror Focus PDF generation (matplotlib unavailable): %s", exc)
        return None

    report_dir.mkdir(parents=True, exist_ok=True)
    market = _read_gold(gold_dir, "gold_market_weekly.csv")
    market["snapshot_date"] = pd.to_datetime(market["snapshot_date"], errors="coerce")
    latest, prev = _safe_prev_date(market["snapshot_date"])
    if latest is None:
        raise ValueError("Unable to determine latest snapshot date.")

    silver = _read_silver(silver_dir)
    table = _build_page1_table(silver=silver, latest=latest, prev=prev)
    week = _week_label(latest)
    output = report_dir / f"offeror_focus_{week}_Poland.pdf"

    display = table.copy()
    source_display = display.copy()
    if display.empty:
        display = pd.DataFrame([{"Status": "No data available for page 1."}])
        source_display = display.copy()
    else:
        display = display.rename(
            columns={
                "segment_reference_group": "Segment Ref Group",
                "line": "Line",
                "key_fitments": "Key Fitments",
                "brand": "Brand",
                "pattern_set": "Pattern Set",
                "size_norm": "Size",
                "ipcode": "Ipcode",
                "first_player": "First Player",
                "stock": "Stock",
                "platforma_price": "Price",
                "disc_pct": "Disc %",
                "price_var_vs_lw_pct": "vs LW",
                "markup_pct": "Mark-Up",
                "markup_var_vs_lw_pp": "Mark-Up vs LW (pp)",
            }
        )[
            [
                "Segment Ref Group",
                "Line",
                "Key Fitments",
                "Brand",
                "Pattern Set",
                "Size",
                "Ipcode",
                "First Player",
                "Stock",
                "Price",
                "Disc %",
                "vs LW",
                "Mark-Up",
                "Mark-Up vs LW (pp)",
            ]
        ]
        display["Key Fitments"] = display["Key Fitments"].map(
            lambda s: str(s).replace(" & ", "\n& ")
        )
        source_display = display.copy()
        display = _collapse_repeated_rows(
            display,
            cols=["Segment Ref Group", "Line", "Key Fitments", "Brand", "Pattern Set"],
        )
        display["Stock"] = display["Stock"].map(lambda v: _fmt_num(v, 0))
        display["Price"] = display["Price"].map(lambda v: _fmt_num(v, 1))
        display["Disc %"] = display["Disc %"].map(lambda v: _fmt_pct(v, 1))
        display["vs LW"] = display["vs LW"].map(lambda v: _fmt_pct(v, 1, signed=True))
        display["Mark-Up"] = display["Mark-Up"].map(lambda v: _fmt_pct(v, 1))
        display["Mark-Up vs LW (pp)"] = display["Mark-Up vs LW (pp)"].map(
            lambda v: _fmt_pct(v, 1, signed=True)
        )

    with PdfPages(output) as pdf:
        fig = plt.figure(figsize=(18, 9))
        ax = fig.add_subplot(111)
        fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
        ax.axis("off")
        fig.suptitle(
            f"OFFEROR FOCUS - WEEK {week.replace('W', '')}",
            fontsize=20,
            fontweight="bold",
            y=0.97,
        )
        fig.text(
            0.5,
            0.94,
            "TOP - KEY FITMENT PRODUCTS (Top 1 Offeror) | Pirelli + Metzeler",
            ha="center",
            va="center",
            fontsize=11,
            color="#374151",
        )

        _draw_pdf_table(
            ax=ax,
            display=display,
            source=source_display,
            bbox=[0.01, 0.05, 0.98, 0.85],
            col_widths=[
                0.145,
                0.028,
                0.128,
                0.048,
                0.082,
                0.086,
                0.056,
                0.112,
                0.035,
                0.05,
                0.042,
                0.042,
                0.044,
                0.072,
            ],
        )

        fig.text(0.03, 0.015, "Page 1", fontsize=8, color="#4B5563")
        pdf.savefig(fig)
        plt.close(fig)

    logger.info("Offeror Focus PDF written: %s", output)
    return output
