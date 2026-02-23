from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .settings import FOCUS_BRANDS, GOLD_DIR, LOGOS_DIR, RECAP_BRANDS, REPORT_DIR, SILVER_DIR


BRAND_COLORS = {
    "Pirelli": "#F4C300",
    "Michelin": "#1E4D8C",
    "Bridgestone": "#E53935",
    "Dunlop": "#F9A825",
    "Continental": "#FF8F00",
}


def _read_gold(gold_dir: Path, filename: str) -> pd.DataFrame:
    path = gold_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Expected mart file not found: {path}")
    return pd.read_csv(path)


def _read_recap_latest(gold_dir: Path) -> pd.DataFrame:
    latest_path = gold_dir / "gold_recap_by_brand_latest.csv"
    if latest_path.exists():
        return pd.read_csv(latest_path)
    weekly_path = gold_dir / "gold_recap_by_brand_weekly.csv"
    if not weekly_path.exists():
        raise FileNotFoundError("Expected recap file not found in gold layer.")
    recap = pd.read_csv(weekly_path)
    recap["snapshot_date"] = pd.to_datetime(recap["snapshot_date"], errors="coerce")
    latest = recap["snapshot_date"].max()
    recap = recap[recap["snapshot_date"] == latest].copy()
    recap["positioning_display"] = recap["positioning_index_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x)}")
    recap["vs_prev_week_display"] = recap["vs_prev_week_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x):+d}")
    recap["vs_py_display"] = recap["vs_py_round"].map(lambda x: "-" if pd.isna(x) else f"{int(x):+d}")
    iso = pd.to_datetime(recap["snapshot_date"], errors="coerce").dt.isocalendar()
    recap["week_label"] = iso["year"].astype("Int64").astype("string") + "-W" + iso["week"].astype("Int64").map(
        lambda x: f"{int(x):02d}" if pd.notna(x) else "--"
    )
    return recap


def _read_silver(silver_dir: Path) -> pd.DataFrame:
    parquet = silver_dir / "motorcycle_weekly.parquet"
    csv_file = silver_dir / "motorcycle_weekly.csv"
    if parquet.exists():
        try:
            return pd.read_parquet(parquet)
        except Exception:
            pass
    if csv_file.exists():
        return pd.read_csv(csv_file, low_memory=False)
    raise FileNotFoundError("No silver dataset found (expected motorcycle_weekly.parquet or .csv).")


def _latest_snapshot(df: pd.DataFrame, date_col: str = "snapshot_date") -> str:
    latest = pd.to_datetime(df[date_col], errors="coerce").max()
    if pd.isna(latest):
        raise ValueError("No valid snapshot date found in marts.")
    return latest.strftime("%Y-%m-%d")


def _week_label(date_str: str) -> str:
    dt = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(dt):
        return "W00"
    return f"W{int(dt.isocalendar().week):02d}"


def _safe_prev_date(series: pd.Series) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    values = sorted(pd.to_datetime(series, errors="coerce").dropna().unique())
    if not values:
        return None, None
    latest = pd.Timestamp(values[-1])
    prev = pd.Timestamp(values[-2]) if len(values) > 1 else None
    return latest, prev


def _brand_color(brand: str) -> str:
    return BRAND_COLORS.get(brand, "#6B7280")


def _logo_path(brand: str, logos_dir: Path = LOGOS_DIR) -> Path | None:
    slug = brand.lower().replace(" ", "_").replace("/", "_")
    for ext in ("png", "jpg", "jpeg"):
        path = logos_dir / f"{slug}.{ext}"
        if path.exists():
            return path
    return None


def _add_logo_or_label(ax, brand: str, x: float, y: float, logos_dir: Path = LOGOS_DIR) -> None:
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage
    import matplotlib.pyplot as plt

    path = _logo_path(brand, logos_dir=logos_dir)
    if path:
        try:
            image = plt.imread(path)
            box = OffsetImage(image, zoom=0.08)
            ax.add_artist(AnnotationBbox(box, (x, y), frameon=False))
            return
        except Exception:
            pass

    ax.text(
        x,
        y,
        brand[:3].upper(),
        va="center",
        ha="center",
        fontsize=8,
        color="#111827",
        bbox={"boxstyle": "round,pad=0.2", "fc": _brand_color(brand), "ec": "#111827", "lw": 0.5},
    )


def _decorate_page(fig, title: str, subtitle: str) -> None:
    fig.subplots_adjust(top=0.84, bottom=0.07, left=0.05, right=0.98)
    fig.suptitle(title, fontsize=17, fontweight="bold", x=0.03, y=0.975, ha="left")
    fig.text(0.03, 0.935, subtitle, fontsize=9.5, color="#374151", ha="left")
    fig.text(
        0.995,
        0.01,
        "Internal analytical draft | Oponeo snapshots | Pirelli + top competitors",
        fontsize=7,
        color="#6B7280",
        ha="right",
    )


def _add_subplot_note(ax, note: str) -> None:
    ax.text(0.0, 1.01, note, transform=ax.transAxes, fontsize=8, color="#4B5563", va="bottom", ha="left")


def _format_date_axis(ax, max_ticks: int = 6) -> None:
    import matplotlib.dates as mdates

    locator = mdates.AutoDateLocator(minticks=2, maxticks=max_ticks)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.tick_params(axis="x", labelsize=8)


def _draw_brand_logo_strip(fig, brands: list[str], logos_dir: Path = LOGOS_DIR) -> None:
    import matplotlib.pyplot as plt

    ax = fig.add_axes([0.62, 0.89, 0.34, 0.06])
    ax.axis("off")
    if not brands:
        return

    xs = np.linspace(0.05, 0.95, len(brands))
    for x, brand in zip(xs, brands):
        path = _logo_path(brand, logos_dir=logos_dir)
        if path:
            try:
                img = plt.imread(path)
                icon_ax = ax.inset_axes([x - 0.05, 0.05, 0.10, 0.90])
                icon_ax.imshow(img)
                icon_ax.set_aspect("auto")
                icon_ax.axis("off")
                continue
            except Exception:
                pass
        ax.text(
            x,
            0.5,
            brand[:3].upper(),
            va="center",
            ha="center",
            fontsize=8,
            color="#111827",
            bbox={"boxstyle": "round,pad=0.18", "fc": _brand_color(brand), "ec": "#111827", "lw": 0.5},
            transform=ax.transAxes,
        )


def _draw_recap_matrix_page(fig, recap_latest: pd.DataFrame, logos_dir: Path = LOGOS_DIR) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    def _place_logo_preserve_ratio(ax, img, x: float, y: float, w: float, h: float) -> None:
        ih, iw = img.shape[:2]
        if ih <= 0 or iw <= 0 or w <= 0 or h <= 0:
            return
        img_ratio = iw / ih
        cell_ratio = w / h
        if img_ratio >= cell_ratio:
            draw_w = w * 0.97
            draw_h = draw_w / img_ratio
        else:
            draw_h = h * 0.90
            draw_w = draw_h * img_ratio
        dx = x + (w - draw_w) / 2
        dy = y + (h - draw_h) / 2
        logo_ax = ax.inset_axes([dx, dy, draw_w, draw_h], transform=ax.transAxes)
        logo_ax.imshow(img)
        logo_ax.set_aspect("equal")
        logo_ax.axis("off")

    fig.subplots_adjust(left=0.04, right=0.96, top=0.95, bottom=0.06)
    ax = fig.add_subplot(111)
    ax.axis("off")

    week_label = recap_latest["week_label"].iloc[0] if not recap_latest.empty and "week_label" in recap_latest.columns else "-"
    title = f"RECAP BY BRAND - Key Fitments, weighted by Pirelli Volumes, {week_label} Poland"
    ax.add_patch(Rectangle((0.02, 0.89), 0.96, 0.08, transform=ax.transAxes, facecolor="#A9CCE3", edgecolor="none"))
    ax.text(0.03, 0.925, title, transform=ax.transAxes, fontsize=17, fontweight="bold", color="#2C3E50", va="center")

    headers = ["Brand", "Positioning", "vs Prev. Week", "vs PY"]
    col_x = [0.24, 0.48, 0.62, 0.76]
    col_w = [0.18, 0.08, 0.08, 0.08]
    for h, x, w in zip(headers, col_x, col_w):
        ax.text(x + w / 2, 0.74, h, transform=ax.transAxes, ha="center", va="bottom", fontsize=12, fontweight="bold")

    rows = recap_latest.copy()
    rows["brand"] = pd.Categorical(rows["brand"], categories=RECAP_BRANDS, ordered=True)
    rows = rows.sort_values("brand")

    y0 = 0.67
    row_h = 0.09
    for i, row in enumerate(rows.itertuples(index=False)):
        y = y0 - i * row_h
        brand = str(row.brand)
        brand_color = _brand_color(brand)

        # Brand cell with logo.
        bx, bw = col_x[0], col_w[0]
        ax.add_patch(Rectangle((bx, y), bw, row_h * 0.78, transform=ax.transAxes, facecolor="#FFFFFF", edgecolor=brand_color, lw=1.4))
        logo = _logo_path(brand, logos_dir=logos_dir)
        if logo:
            try:
                img = plt.imread(logo)
                _place_logo_preserve_ratio(ax, img, bx + 0.005, y + 0.004, bw - 0.010, row_h * 0.78 - 0.008)
                ax.text(
                    bx + bw / 2,
                    y + 0.01,
                    brand.upper(),
                    transform=ax.transAxes,
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color="#374151",
                )
            except Exception:
                ax.text(bx + bw / 2, y + row_h * 0.39, brand.upper(), transform=ax.transAxes, ha="center", va="center", fontsize=12)
        else:
            ax.text(bx + bw / 2, y + row_h * 0.39, brand.upper(), transform=ax.transAxes, ha="center", va="center", fontsize=12)

        values = [str(row.positioning_display), str(row.vs_prev_week_display), str(row.vs_py_display)]
        for j, val in enumerate(values, start=1):
            x, w = col_x[j], col_w[j]
            ax.add_patch(Rectangle((x, y), w, row_h * 0.78, transform=ax.transAxes, facecolor="#F4F6F7", edgecolor=brand_color, lw=1.2))
            ax.text(x + w / 2, y + row_h * 0.39, val, transform=ax.transAxes, ha="center", va="center", fontsize=17, color="#566573")


def _build_positioning_across_lines_latest(
    silver: pd.DataFrame, latest: pd.Timestamp, brands: list[str] | None = None
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    if brands is None:
        brands = list(RECAP_BRANDS)

    work = silver.copy()
    if "snapshot_date" not in work.columns or "segment_reference_group" not in work.columns:
        return {}, brands

    is_hc = work.get("is_high_confidence_match")
    if is_hc is not None:
        work = work[is_hc.fillna(False)]
    work = work[work["snapshot_date"] == latest].copy()
    work = work[work["brand"].isin(brands)].copy()
    if work.empty:
        return {}, brands

    parsed = (
        work["segment_reference_group"]
        .astype("string")
        .str.upper()
        .str.extract(r"^(?:\s*\d+\s*-\s*)?(?P<segment>.+?)\s+(?P<line>1ST|2ND|3RD)\s*$")
    )
    work["segment"] = parsed["segment"].astype("string").str.strip()
    work["line"] = parsed["line"].astype("string").str.strip()
    work = work[work["segment"].isin(["SUPERSPORT", "SPORT TOURING RADIAL"])].copy()
    if work.empty:
        return {}, brands

    agg = (
        work.groupby(["segment", "line", "brand"], dropna=False)
        .agg(price=("price_pln", "median"))
        .reset_index()
    )
    bases = (
        agg[(agg["brand"] == "Pirelli") & (agg["line"] == "1ST")][["segment", "price"]]
        .rename(columns={"price": "pirelli_1st_price"})
        .drop_duplicates("segment")
    )
    agg = agg.merge(bases, on="segment", how="left")
    agg["index_vs_pirelli_1st"] = 100 * (agg["price"] / agg["pirelli_1st_price"])

    out: dict[str, pd.DataFrame] = {}
    for segment in ["SUPERSPORT", "SPORT TOURING RADIAL"]:
        seg = agg[agg["segment"] == segment].copy()
        if seg.empty:
            continue
        price_p = seg.pivot(index="line", columns="brand", values="price")
        idx_p = seg.pivot(index="line", columns="brand", values="index_vs_pirelli_1st")
        lines = ["1ST", "2ND", "3RD"]
        grid = pd.DataFrame(index=lines)
        for b in brands:
            grid[(b, "price")] = price_p[b] if b in price_p.columns else np.nan
            grid[(b, "index")] = idx_p[b] if b in idx_p.columns else np.nan
        out[segment] = grid
    return out, brands


def _draw_positioning_across_lines_page(
    fig, silver: pd.DataFrame, latest: pd.Timestamp, logos_dir: Path = LOGOS_DIR
) -> None:
    from matplotlib.gridspec import GridSpec

    def _fmt_price(v: float) -> str:
        if pd.isna(v):
            return "-"
        return f"{float(v):.1f}".replace(".", ",")

    def _fmt_idx(v: float) -> str:
        if pd.isna(v):
            return "-"
        return f"{int(round(float(v)))}"

    tables, brands = _build_positioning_across_lines_latest(silver=silver, latest=latest, brands=list(RECAP_BRANDS))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.96, bottom=0.05)
    fig.text(0.5, 0.95, "POSITIONING ACROSS LINES", ha="center", va="center", fontsize=27, color="#D70000", fontweight="bold")
    fig.text(0.5, 0.905, "First 3 Offerors price on Market Segment's key fitment", ha="center", va="center", fontsize=18, color="#7A8793")

    gs = GridSpec(2, 1, figure=fig, hspace=0.17, top=0.84, bottom=0.06)

    def _build_table_ax(ax, segment: str) -> None:
        ax.axis("off")
        seg_tbl = tables.get(segment)
        if seg_tbl is None:
            ax.text(0.5, 0.5, f"No data for {segment}", ha="center", va="center", fontsize=12)
            return

        col_labels = ["Lines"]
        for b in brands:
            col_labels.extend([f"{b}\nPRICE", "INDEX\nPI1=100"])

        row_labels = [("1ST", "1st Line"), ("2ND", "2nd Line"), ("3RD", "3rd Line")]
        cell_text: list[list[str]] = []
        for line_key, line_label in row_labels:
            row = [line_label]
            for b in brands:
                price_col = (b, "price")
                idx_col = (b, "index")
                price = seg_tbl.at[line_key, price_col] if (line_key in seg_tbl.index and price_col in seg_tbl.columns) else np.nan
                idxv = seg_tbl.at[line_key, idx_col] if (line_key in seg_tbl.index and idx_col in seg_tbl.columns) else np.nan
                row.extend([_fmt_price(price), _fmt_idx(idxv)])
            cell_text.append(row)

        ncols = len(col_labels)
        first_w = 0.09
        other_w = (1.0 - first_w) / (ncols - 1)
        col_widths = [first_w] + [other_w] * (ncols - 1)

        table = ax.table(
            cellText=cell_text,
            colLabels=col_labels,
            loc="center",
            cellLoc="center",
            colLoc="center",
            colWidths=col_widths,
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8.6)
        table.scale(1.0, 2.05)

        for (r, c), cell in table.get_celld().items():
            cell.set_edgecolor("#222222")
            cell.set_linewidth(0.8)
            if r == 0:
                cell.set_facecolor("#EDEDED")
                cell.get_text().set_fontweight("bold")
                cell.get_text().set_fontsize(7.0)
                cell.set_height(cell.get_height() * 1.28)
            elif c > 0 and c % 2 == 0:
                cell.set_facecolor("#E8EFDF")
            else:
                cell.set_facecolor("#FFFFFF")

        ax.set_title(segment, fontsize=12, fontweight="bold", loc="left", pad=7)

    _build_table_ax(fig.add_subplot(gs[0, 0]), "SUPERSPORT")
    _build_table_ax(fig.add_subplot(gs[1, 0]), "SPORT TOURING RADIAL")


def _kpi_card(ax, title: str, value: str, delta: str | None, tone: str = "neutral") -> None:
    tones = {
        "good": "#166534",
        "bad": "#991B1B",
        "neutral": "#1F2937",
    }
    ax.axis("off")
    ax.set_facecolor("#F9FAFB")
    ax.text(0.02, 0.78, title, fontsize=9, color="#4B5563", transform=ax.transAxes)
    ax.text(0.02, 0.35, value, fontsize=18, fontweight="bold", color="#111827", transform=ax.transAxes)
    if delta:
        ax.text(0.02, 0.1, delta, fontsize=10, color=tones.get(tone, "#1F2937"), transform=ax.transAxes)


def _pivot_heatmap(ax, df: pd.DataFrame, value_col: str, title: str, fmt: str = "{:.1f}") -> None:
    if df.empty:
        ax.axis("off")
        ax.set_title(f"{title}\n(no data)")
        return

    work = df.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"], errors="coerce")
    key_col = "analysis_fitment_key" if "analysis_fitment_key" in work.columns else "rim_group"
    rows_order = ["<=13", "14-16", "17", "18", "19+", "ALL"]
    work[key_col] = work[key_col].astype("string")
    if work[key_col].isin(rows_order).any():
        work[key_col] = pd.Categorical(work[key_col], categories=rows_order, ordered=True)
    pivot = (
        work.pivot_table(index=key_col, columns="snapshot_date", values=value_col, aggfunc="mean")
        .sort_index()
        .dropna(how="all")
    )
    if pivot.empty:
        ax.axis("off")
        ax.set_title(f"{title}\n(no data)")
        return

    values = pivot.to_numpy(dtype=float)
    im = ax.imshow(values, aspect="auto", cmap="RdYlGn")
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels([str(x) for x in pivot.index], fontsize=8)
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels([pd.Timestamp(x).strftime("%d-%b") for x in pivot.columns], rotation=0, ha="center", fontsize=8)

    for i in range(values.shape[0]):
        for j in range(values.shape[1]):
            if np.isnan(values[i, j]):
                continue
            ax.text(j, i, fmt.format(values[i, j]), ha="center", va="center", fontsize=7, color="#111827")

    ax.figure.colorbar(im, ax=ax, fraction=0.04, pad=0.02)


def _build_key_fitment_table(silver: pd.DataFrame, latest: pd.Timestamp, prev: pd.Timestamp | None) -> pd.DataFrame:
    silver = silver.copy()
    silver["snapshot_date"] = pd.to_datetime(silver["snapshot_date"], errors="coerce")
    silver["stock_qty"] = pd.to_numeric(silver.get("stock_qty"), errors="coerce").fillna(0)
    silver["price_pln"] = pd.to_numeric(silver["price_pln"], errors="coerce")

    latest_df = silver[(silver["snapshot_date"] == latest) & (silver["brand"].isin(FOCUS_BRANDS))].copy()
    if latest_df.empty:
        return pd.DataFrame()

    group_cols = ["brand", "pattern_family", "size_norm"]
    latest_agg = (
        latest_df.groupby(group_cols, dropna=False)
        .agg(stock_qty=("stock_qty", "sum"), rows=("product_code", "count"), median_price=("price_pln", "median"))
        .reset_index()
    )

    seller_rank = (
        latest_df.groupby(group_cols + ["seller_norm"], dropna=False)["stock_qty"]
        .sum()
        .reset_index()
        .sort_values("stock_qty", ascending=False)
    )
    top_seller = seller_rank.drop_duplicates(group_cols, keep="first")[group_cols + ["seller_norm"]]
    latest_agg = latest_agg.merge(top_seller, on=group_cols, how="left")

    if prev is not None:
        prev_df = silver[(silver["snapshot_date"] == prev) & (silver["brand"].isin(FOCUS_BRANDS))].copy()
        prev_agg = (
            prev_df.groupby(group_cols, dropna=False)
            .agg(prev_median_price=("price_pln", "median"), prev_rows=("product_code", "count"))
            .reset_index()
        )
        latest_agg = latest_agg.merge(prev_agg, on=group_cols, how="left")
    else:
        latest_agg["prev_median_price"] = np.nan
        latest_agg["prev_rows"] = np.nan

    latest_agg["wow_price_delta"] = latest_agg["median_price"] - latest_agg["prev_median_price"]
    latest_agg["wow_rows_delta"] = latest_agg["rows"] - latest_agg["prev_rows"]

    top_by_brand = (
        latest_agg.sort_values(["brand", "stock_qty"], ascending=[True, False]).groupby("brand", as_index=False).head(5)
    )
    return top_by_brand.sort_values(["brand", "stock_qty"], ascending=[True, False])


def build_excel_report(logger: logging.Logger, gold_dir: Path = GOLD_DIR, report_dir: Path = REPORT_DIR) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)

    market = _read_gold(gold_dir, "gold_market_weekly.csv")
    brand = _read_gold(gold_dir, "gold_brand_weekly.csv")
    segment = _read_gold(gold_dir, "gold_segment_weekly.csv")
    seller = _read_gold(gold_dir, "gold_seller_weekly.csv")
    fitment = _read_gold(gold_dir, "gold_fitment_weekly.csv")
    positioning = _read_gold(gold_dir, "gold_price_positioning_weekly.csv")

    latest = _latest_snapshot(market)
    week_label = _week_label(latest)
    output = report_dir / f"PRICE_POSITIONING_{week_label}_Poland.xlsx"

    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            market.to_excel(writer, sheet_name="Exec_Summary", index=False)
            positioning.to_excel(writer, sheet_name="Price_Positioning", index=False)
            brand.to_excel(writer, sheet_name="Competitors", index=False)
            fitment.to_excel(writer, sheet_name="Fitment", index=False)
            seller.to_excel(writer, sheet_name="Sellers", index=False)
            segment.to_excel(writer, sheet_name="Rim_Segments", index=False)
        logger.info("Excel report written: %s", output)
        return output
    except Exception as exc:  # pragma: no cover
        fallback_dir = report_dir / f"PRICE_POSITIONING_{week_label}_Poland_csv"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        market.to_csv(fallback_dir / "Exec_Summary.csv", index=False)
        positioning.to_csv(fallback_dir / "Price_Positioning.csv", index=False)
        brand.to_csv(fallback_dir / "Competitors.csv", index=False)
        fitment.to_csv(fallback_dir / "Fitment.csv", index=False)
        seller.to_csv(fallback_dir / "Sellers.csv", index=False)
        segment.to_csv(fallback_dir / "Rim_Segments.csv", index=False)
        logger.warning("Excel generation failed (%s). Wrote CSV report bundle at %s", exc, fallback_dir)
        return fallback_dir


def build_pdf_report(
    logger: logging.Logger,
    gold_dir: Path = GOLD_DIR,
    report_dir: Path = REPORT_DIR,
    silver_dir: Path = SILVER_DIR,
    logos_dir: Path = LOGOS_DIR,
) -> Path | None:
    try:
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
        from matplotlib.gridspec import GridSpec
        from matplotlib.ticker import MaxNLocator
    except Exception as exc:  # pragma: no cover
        logger.warning("Skipping PDF generation (matplotlib unavailable): %s", exc)
        return None

    report_dir.mkdir(parents=True, exist_ok=True)
    market = _read_gold(gold_dir, "gold_market_weekly.csv")
    brand = _read_gold(gold_dir, "gold_brand_weekly.csv")
    segment = _read_gold(gold_dir, "gold_segment_weekly.csv")
    seller = _read_gold(gold_dir, "gold_seller_weekly.csv")
    fitment = _read_gold(gold_dir, "gold_fitment_weekly.csv")
    positioning = _read_gold(gold_dir, "gold_price_positioning_weekly.csv")
    recap_latest = _read_recap_latest(gold_dir)
    silver = _read_silver(silver_dir)

    for df in (market, brand, segment, seller, fitment, positioning, silver):
        if "snapshot_date" in df.columns:
            df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")

    latest, prev = _safe_prev_date(market["snapshot_date"])
    if latest is None:
        raise ValueError("Unable to determine latest snapshot date.")

    week_label = f"W{int(latest.isocalendar().week):02d}"
    output = report_dir / f"PRICE_POSITIONING_{week_label}_Poland.pdf"
    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.facecolor": "#FFFFFF",
            "figure.facecolor": "#FFFFFF",
            "axes.edgecolor": "#D1D5DB",
            "axes.titleweight": "bold",
        }
    )

    with PdfPages(output) as pdf:
        # Page 1: Italy-style recap matrix.
        fig = plt.figure(figsize=(16, 9))
        _draw_recap_matrix_page(fig, recap_latest=recap_latest, logos_dir=logos_dir)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 2: Positioning across lines (Supersport, Sport Touring Radial).
        fig = plt.figure(figsize=(16, 9))
        _draw_positioning_across_lines_page(fig, silver=silver, latest=latest, logos_dir=logos_dir)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 3: Price positioning and segment heatmaps
        fig = plt.figure(figsize=(16, 9))
        gs = GridSpec(2, 3, figure=fig, height_ratios=[1.2, 1.3], hspace=0.4, wspace=0.28)
        _decorate_page(fig, "Price Positioning", "Gap = Pirelli median price - median of top competitor set, by week and rim group")

        overall = positioning[positioning["granularity"] == "overall"].sort_values("snapshot_date")
        ax = fig.add_subplot(gs[0, :2])
        ax.plot(overall["snapshot_date"], overall["price_gap_vs_comp"], color="#111827", linewidth=2.5, marker="o")
        ax.axhline(0, color="#6B7280", linewidth=1)
        ax.fill_between(
            overall["snapshot_date"],
            overall["price_gap_vs_comp"],
            0,
            where=overall["price_gap_vs_comp"] >= 0,
            color="#22C55E",
            alpha=0.15,
        )
        ax.fill_between(
            overall["snapshot_date"],
            overall["price_gap_vs_comp"],
            0,
            where=overall["price_gap_vs_comp"] < 0,
            color="#EF4444",
            alpha=0.15,
        )
        ax.set_title("Pirelli Price Gap vs Top Competitors (Median)")
        _add_subplot_note(ax, "Positive = Pirelli priced above competitor median. Negative = below competitor median.")
        _format_date_axis(ax, max_ticks=5)
        ax.set_ylabel("Gap (PLN)")
        ax.yaxis.set_major_locator(MaxNLocator(nbins=6))
        ax.grid(alpha=0.25)
        for row in overall.itertuples(index=False):
            ax.text(row.snapshot_date, row.price_gap_vs_comp, f"{row.price_gap_vs_comp:+.1f}", fontsize=8, ha="center", va="bottom")

        ax_tbl = fig.add_subplot(gs[0, 2])
        ax_tbl.axis("off")
        segment_granularity = "fitment_size_root" if "fitment_size_root" in positioning["granularity"].astype("string").unique() else "rim_group"
        key_col = "analysis_fitment_key" if "analysis_fitment_key" in positioning.columns else "rim_group"
        latest_pos = positioning[positioning["snapshot_date"] == latest].copy()
        latest_pos = latest_pos[latest_pos["granularity"] == segment_granularity].sort_values("price_gap_vs_comp", ascending=False)
        table_cols = [key_col, "pirelli_median_price", "competitor_median_price", "price_gap_vs_comp", "pirelli_price_index"]
        table_df = latest_pos[table_cols].copy()
        table_df.columns = ["Fitment/Size", "Pirelli", "Competitors", "Gap", "Index"]
        table_df = table_df.head(10)
        for col in ["Pirelli", "Competitors", "Gap", "Index"]:
            table_df[col] = table_df[col].map(lambda x: f"{x:.1f}" if pd.notna(x) else "n/a")
        tbl = ax_tbl.table(cellText=table_df.values, colLabels=table_df.columns, loc="center", cellLoc="center")
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        tbl.scale(1.0, 1.2)
        ax_tbl.set_title("Latest Week by Rim Group", pad=8)

        ax_h1 = fig.add_subplot(gs[1, 0:2])
        _pivot_heatmap(
            ax_h1,
            positioning[positioning["granularity"] == segment_granularity],
            value_col="price_gap_vs_comp",
            title="Heatmap: Pirelli Gap vs Competitors (PLN) by Fitment/Size",
            fmt="{:+.1f}",
        )
        ax_h2 = fig.add_subplot(gs[1, 2])
        _pivot_heatmap(
            ax_h2,
            positioning[positioning["granularity"] == segment_granularity],
            value_col="pirelli_price_index",
            title="Heatmap: Pirelli Price Index by Fitment/Size",
            fmt="{:.1f}",
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 4: Fitment and segment dynamics
        fig = plt.figure(figsize=(16, 9))
        gs = GridSpec(2, 2, figure=fig, hspace=0.35, wspace=0.25)
        _decorate_page(
            fig,
            "Fitment and Segment Dynamics",
            "Rows-based fitment mix and rim exposure across Pirelli and key competitors (latest snapshot)",
        )

        latest_fit = fitment[fitment["snapshot_date"] == latest].copy()
        latest_fit = latest_fit[latest_fit["brand"].isin(FOCUS_BRANDS)]
        mix = latest_fit.pivot_table(index="brand", columns="fitment_position", values="rows", aggfunc="sum", fill_value=0)
        mix = mix.reindex(FOCUS_BRANDS).fillna(0)
        ax = fig.add_subplot(gs[0, 0])
        left = np.zeros(len(mix))
        for col in ["Front", "Rear", "Unknown"]:
            vals = mix[col].to_numpy() if col in mix.columns else np.zeros(len(mix))
            ax.barh(mix.index, vals, left=left, label=col, alpha=0.9)
            left = left + vals
        ax.set_title("Fitment Position Mix by Brand (Rows)")
        _add_subplot_note(ax, "Fitment inferred from product name token: FRONT/REAR; remaining entries marked Unknown.")
        ax.set_xlabel("Rows")
        ax.legend(frameon=False, ncol=3)
        ax.grid(axis="x", alpha=0.25)

        latest_seg = segment[segment["snapshot_date"] == latest].copy()
        latest_seg = latest_seg[latest_seg["brand"].isin(FOCUS_BRANDS)]
        seg_key = "analysis_fitment_key" if "analysis_fitment_key" in latest_seg.columns else "rim_group"
        seg_pivot = latest_seg.pivot_table(index=seg_key, columns="brand", values="rows", aggfunc="sum", fill_value=0)
        seg_pivot = seg_pivot.sort_values(by="Pirelli", ascending=False) if "Pirelli" in seg_pivot.columns else seg_pivot
        seg_pivot = seg_pivot.head(8)
        ax2 = fig.add_subplot(gs[0, 1])
        x = np.arange(len(seg_pivot.index))
        width = 0.15
        for i, b in enumerate(FOCUS_BRANDS):
            vals = seg_pivot[b].to_numpy() if b in seg_pivot.columns else np.zeros(len(x))
            ax2.bar(x + (i - 2) * width, vals, width=width, color=_brand_color(b), label=b, alpha=0.85)
        ax2.set_xticks(x)
        ax2.set_xticklabels(seg_pivot.index)
        ax2.set_title("Top Fitment/Size Exposure by Brand (Rows)")
        _add_subplot_note(ax2, "Focus on top fitment/size keys by Pirelli row footprint.")
        ax2.set_ylabel("Rows")
        ax2.yaxis.set_major_locator(MaxNLocator(nbins=6, integer=True))
        ax2.grid(axis="y", alpha=0.25)
        ax2.legend(frameon=False, fontsize=8, ncol=3)

        latest_brand_view = brand[brand["snapshot_date"] == latest][["brand", "rows", "unique_sellers", "stock_qty", "median_price"]].copy()
        latest_brand_view = latest_brand_view.sort_values("rows", ascending=False)
        ax3 = fig.add_subplot(gs[1, :])
        ax3.axis("off")
        tb = latest_brand_view.copy()
        tb["rows"] = tb["rows"].map("{:,.0f}".format)
        tb["unique_sellers"] = tb["unique_sellers"].map("{:,.0f}".format)
        tb["stock_qty"] = tb["stock_qty"].map(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
        tb["median_price"] = tb["median_price"].map("{:.1f}".format)
        tb.columns = ["Brand", "Rows", "Sellers", "Stock", "Median Price"]
        table = ax3.table(cellText=tb.values, colLabels=tb.columns, loc="center", cellLoc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.35)
        ax3.set_title("Competitor Checkpoint - Latest Week Summary")
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 5: Seller checkpoint with bubble chart
        fig = plt.figure(figsize=(16, 9))
        gs = GridSpec(2, 2, figure=fig, height_ratios=[1.25, 1.0], hspace=0.35, wspace=0.25)
        _decorate_page(
            fig,
            "Seller Focus",
            "Top offerors by listing volume with stock-weighted footprint and Pirelli concentration",
        )

        latest_seller = seller[seller["snapshot_date"] == latest].copy()
        latest_seller = latest_seller[latest_seller["brand"].isin(FOCUS_BRANDS)]
        seller_rollup = (
            latest_seller.groupby("seller_norm", dropna=False)
            .agg(rows=("rows", "sum"), stock_qty=("stock_qty", "sum"), median_price=("median_price", "median"))
            .reset_index()
        )
        pirelli_seller = latest_seller[latest_seller["brand"] == "Pirelli"][["seller_norm", "rows"]].rename(
            columns={"rows": "pirelli_rows"}
        )
        seller_rollup = seller_rollup.merge(pirelli_seller, on="seller_norm", how="left").fillna({"pirelli_rows": 0})
        seller_rollup["pirelli_share"] = np.where(
            seller_rollup["rows"] > 0, seller_rollup["pirelli_rows"] / seller_rollup["rows"], 0
        )
        top_sellers = seller_rollup.sort_values("rows", ascending=False).head(25)

        ax = fig.add_subplot(gs[0, :])
        sizes = np.clip(top_sellers["stock_qty"].to_numpy() / 20.0, 40, 3000)
        scatter = ax.scatter(
            top_sellers["median_price"],
            top_sellers["rows"],
            s=sizes,
            c=top_sellers["pirelli_share"],
            cmap="YlOrRd",
            alpha=0.65,
            edgecolors="#1F2937",
            linewidth=0.5,
        )
        ax.set_title("Top Sellers Bubble Map | size = stock, color = Pirelli share")
        _add_subplot_note(
            ax,
            "Each bubble is a seller. X: median price (PLN), Y: total rows, size: stock quantity, color: Pirelli row share.",
        )
        ax.set_xlabel("Median Price (PLN)")
        ax.set_ylabel("Rows")
        ax.yaxis.set_major_locator(MaxNLocator(nbins=6, integer=True))
        ax.grid(alpha=0.25)
        for row in top_sellers.head(8).itertuples(index=False):
            ax.text(row.median_price, row.rows, str(row.seller_norm)[:28], fontsize=7, ha="left", va="bottom")
        fig.colorbar(scatter, ax=ax, fraction=0.02, pad=0.01, label="Pirelli share")

        ax2 = fig.add_subplot(gs[1, 0])
        table_df = top_sellers.sort_values("pirelli_rows", ascending=False).head(12).copy()
        table_df = table_df[["seller_norm", "pirelli_rows", "rows", "pirelli_share", "median_price"]]
        table_df["pirelli_share"] = (table_df["pirelli_share"] * 100).map(lambda x: f"{x:.1f}%")
        table_df["median_price"] = table_df["median_price"].map(lambda x: f"{x:.1f}")
        table_df.columns = ["Seller", "Pirelli Rows", "Total Rows", "Pirelli Share", "Median Price"]
        ax2.axis("off")
        t = ax2.table(cellText=table_df.values, colLabels=table_df.columns, cellLoc="center", loc="center")
        t.auto_set_font_size(False)
        t.set_fontsize(8)
        t.scale(1, 1.25)
        ax2.set_title("Top 12 Sellers by Pirelli Rows")

        ax3 = fig.add_subplot(gs[1, 1])
        pirelli_only = latest_seller[latest_seller["brand"] == "Pirelli"].sort_values("rows", ascending=True).tail(12)
        ax3.barh(pirelli_only["seller_norm"].str.slice(0, 26), pirelli_only["rows"], color="#F4C300", edgecolor="#1F2937")
        ax3.set_title("Pirelli Rows by Seller (Top 12)")
        ax3.grid(axis="x", alpha=0.25)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 6: Key fitment product checkpoint
        fig = plt.figure(figsize=(16, 9))
        gs = GridSpec(2, 1, figure=fig, height_ratios=[0.35, 1.65], hspace=0.18)
        _decorate_page(
            fig,
            "Key Fitment Checkpoint",
            "Top stocked fitments by brand (pattern + size), including latest price level and WoW movement",
        )

        key_fit = _build_key_fitment_table(silver, latest=latest, prev=prev)
        ax_head = fig.add_subplot(gs[0, 0])
        ax_head.axis("off")
        ax_head.text(
            0.01,
            0.65,
            "Method: grouped by brand + pattern family + size. Top 5 per brand by latest stock.",
            fontsize=10,
            color="#374151",
            transform=ax_head.transAxes,
        )
        ax_head.text(
            0.01,
            0.2,
            "Columns align to the Italian checkpoint logic: offeror, stock, price level, and week-over-week movement.",
            fontsize=10,
            color="#374151",
            transform=ax_head.transAxes,
        )

        ax_tbl = fig.add_subplot(gs[1, 0])
        ax_tbl.axis("off")
        if key_fit.empty:
            ax_tbl.text(0.5, 0.5, "No key fitment data available.", ha="center", va="center")
        else:
            show = key_fit.copy()
            show["stock_qty"] = show["stock_qty"].map(lambda x: f"{x:,.0f}")
            show["median_price"] = show["median_price"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "n/a")
            show["wow_price_delta"] = show["wow_price_delta"].map(lambda x: f"{x:+.1f}" if pd.notna(x) else "n/a")
            show["wow_rows_delta"] = show["wow_rows_delta"].map(lambda x: f"{x:+.0f}" if pd.notna(x) else "n/a")
            show = show[
                [
                    "brand",
                    "pattern_family",
                    "size_norm",
                    "seller_norm",
                    "stock_qty",
                    "rows",
                    "median_price",
                    "wow_price_delta",
                    "wow_rows_delta",
                ]
            ]
            show.columns = [
                "Brand",
                "Pattern",
                "Size",
                "Top Seller",
                "Stock",
                "Rows",
                "Median Price",
                "WoW Price",
                "WoW Rows",
            ]
            tbl = ax_tbl.table(cellText=show.values, colLabels=show.columns, cellLoc="center", loc="center")
            tbl.auto_set_font_size(False)
            tbl.set_fontsize(8)
            tbl.scale(1.0, 1.18)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    logger.info("PDF report written: %s", output)
    return output
