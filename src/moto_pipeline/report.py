from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .canonical import load_canonical_mapping
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


def _build_segment_pattern_checkpoint(
    silver: pd.DataFrame,
    segment_reference_group: str,
    latest: pd.Timestamp,
    prev: pd.Timestamp | None,
    brands: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if brands is None:
        brands = list(RECAP_BRANDS)

    work = silver.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"], errors="coerce")
    work["price_pln"] = pd.to_numeric(work["price_pln"], errors="coerce")
    work["stock_qty"] = pd.to_numeric(work.get("stock_qty"), errors="coerce").fillna(0)
    work["pattern_set"] = work.get("pattern_set", pd.Series(index=work.index, dtype="string")).astype("string").str.strip()
    work = work[work["brand"].isin(brands)]
    work = work[work["segment_reference_group"].astype("string").str.strip() == segment_reference_group]
    work = work[work["price_pln"].notna()]
    if "is_high_confidence_match" in work.columns:
        work = work[work["is_high_confidence_match"].fillna(False)]
    work = work[work["pattern_set"].notna() & (work["pattern_set"] != "")]
    if work.empty:
        return pd.DataFrame(), pd.DataFrame()

    latest_slice = work[work["snapshot_date"] == latest].copy()
    if latest_slice.empty:
        return pd.DataFrame(), pd.DataFrame()

    top_patterns = (
        latest_slice.groupby(["brand", "pattern_set"], dropna=False)
        .agg(stock_qty=("stock_qty", "sum"), rows=("product_code", "count"), price_cw=("price_pln", "median"))
        .reset_index()
        .sort_values(["brand", "stock_qty", "rows", "price_cw", "pattern_set"], ascending=[True, False, False, False, True])
        .groupby("brand", as_index=False)
        .head(1)
    )
    brand_to_pattern = dict(zip(top_patterns["brand"], top_patterns["pattern_set"]))

    series_rows: list[dict[str, object]] = []
    rows: list[dict[str, object]] = []
    latest_iso = latest.isocalendar()
    py_year = int(latest_iso.year) - 1
    py_week = int(latest_iso.week)

    for brand in brands:
        pattern = brand_to_pattern.get(brand)
        record = {
            "brand": brand,
            "pattern_set": "-" if pattern is None else str(pattern),
            "price_py": np.nan,
            "price_lw": np.nan,
            "price_cw": np.nan,
        }
        if pattern is None:
            rows.append(record)
            continue

        bp = work[(work["brand"] == brand) & (work["pattern_set"] == pattern)].copy()
        ts = bp.groupby("snapshot_date", dropna=False).agg(price=("price_pln", "median")).reset_index().sort_values("snapshot_date")
        if ts.empty:
            rows.append(record)
            continue

        for r in ts.itertuples(index=False):
            series_rows.append(
                {
                    "brand": brand,
                    "pattern_set": pattern,
                    "snapshot_date": r.snapshot_date,
                    "price": r.price,
                }
            )

        cw_row = ts[ts["snapshot_date"] == latest]
        if not cw_row.empty:
            record["price_cw"] = float(cw_row["price"].iloc[0])
        if prev is not None:
            lw_row = ts[ts["snapshot_date"] == prev]
            if not lw_row.empty:
                record["price_lw"] = float(lw_row["price"].iloc[0])
        iso = ts["snapshot_date"].dt.isocalendar()
        py_row = ts[(iso["year"] == py_year) & (iso["week"] == py_week)]
        if not py_row.empty:
            record["price_py"] = float(py_row["price"].iloc[-1])

        rows.append(record)

    table = pd.DataFrame(rows)
    series_df = pd.DataFrame(series_rows)

    pirelli_row = table[table["brand"] == "Pirelli"]
    pirelli_cw = float(pirelli_row["price_cw"].iloc[0]) if not pirelli_row.empty and pd.notna(pirelli_row["price_cw"].iloc[0]) else np.nan
    pirelli_lw = float(pirelli_row["price_lw"].iloc[0]) if not pirelli_row.empty and pd.notna(pirelli_row["price_lw"].iloc[0]) else np.nan

    table["index_cw"] = np.where(
        pd.notna(table["price_cw"]) & pd.notna(pirelli_cw) & (pirelli_cw != 0),
        100 * (table["price_cw"] / pirelli_cw),
        np.nan,
    )
    table["index_lw"] = np.where(
        pd.notna(table["price_lw"]) & pd.notna(pirelli_lw) & (pirelli_lw != 0),
        100 * (table["price_lw"] / pirelli_lw),
        np.nan,
    )
    table["delta_index"] = table["index_cw"] - table["index_lw"]
    table["var_vs_py_pct"] = np.where(
        pd.notna(table["price_cw"]) & pd.notna(table["price_py"]) & (table["price_py"] != 0),
        100 * (table["price_cw"] / table["price_py"] - 1),
        np.nan,
    )
    table["var_vs_lw_pct"] = np.where(
        pd.notna(table["price_cw"]) & pd.notna(table["price_lw"]) & (table["price_lw"] != 0),
        100 * (table["price_cw"] / table["price_lw"] - 1),
        np.nan,
    )
    return table, series_df


def _draw_segment_pattern_checkpoint_page(
    fig,
    silver: pd.DataFrame,
    latest: pd.Timestamp,
    prev: pd.Timestamp | None,
    segment_reference_group: str,
) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec
    from matplotlib.patches import FancyBboxPatch
    from matplotlib.ticker import MaxNLocator

    def _p(v: float) -> str:
        return "-" if pd.isna(v) else f"{float(v):.1f}".replace(".", ",")

    def _idx(v: float) -> str:
        return "-" if pd.isna(v) else f"{int(round(float(v)))}"

    def _delta(v: float) -> str:
        return "-" if pd.isna(v) else f"{int(round(float(v))):+d}"

    def _pct_arrow(v: float) -> str:
        if pd.isna(v):
            return "-"
        if float(v) > 0.05:
            return f"↑ {float(v):+.1f}%"
        if float(v) < -0.05:
            return f"↓ {float(v):+.1f}%"
        return f"→ {float(v):+.1f}%"

    table, series_df = _build_segment_pattern_checkpoint(
        silver=silver,
        segment_reference_group=segment_reference_group,
        latest=latest,
        prev=prev,
        brands=list(RECAP_BRANDS),
    )

    seg_label = str(segment_reference_group).split(" - ", 1)[-1] if " - " in str(segment_reference_group) else str(segment_reference_group)
    _decorate_page(
        fig,
        f"Segment Checkpoint - {seg_label.title()}",
        "Brand -> key pattern set (latest) with PY/LW/CW prices, index vs Pirelli, and weekly trend",
    )
    gs = GridSpec(2, 1, figure=fig, height_ratios=[1.05, 1.20], hspace=0.30)

    ax_tbl = fig.add_subplot(gs[0, 0])
    ax_tbl.axis("off")

    if table.empty:
        ax_tbl.text(0.5, 0.5, f"No high-confidence data for {seg_label}.", ha="center", va="center", fontsize=11)
    else:
        # Rounded container similar to the Italian look.
        ax_tbl.add_patch(
            FancyBboxPatch(
                (0.12, 0.10),
                0.76,
                0.80,
                boxstyle="round,pad=0.012,rounding_size=0.02",
                transform=ax_tbl.transAxes,
                facecolor="#F8FAFC",
                edgecolor="#C9D2DC",
                linewidth=1.0,
                zorder=0,
            )
        )

        disp = table.copy()
        disp["Brand"] = disp["brand"]
        disp["Pattern Set"] = disp["pattern_set"]
        disp["Price PY"] = disp["price_py"].map(_p)
        disp["Price LW"] = disp["price_lw"].map(_p)
        disp["Price CW"] = disp["price_cw"].map(_p)
        disp["Index CW\n(Pirelli=100)"] = disp["index_cw"].map(_idx)
        disp["Delta\nIndex"] = disp["delta_index"].map(_delta)
        disp["% vs PY"] = disp["var_vs_py_pct"].map(_pct_arrow)
        disp["% vs LW"] = disp["var_vs_lw_pct"].map(_pct_arrow)
        disp = disp[
            [
                "Brand",
                "Pattern Set",
                "Price PY",
                "Price LW",
                "Price CW",
                "Index CW\n(Pirelli=100)",
                "Delta\nIndex",
                "% vs PY",
                "% vs LW",
            ]
        ]

        col_widths = [0.11, 0.20, 0.09, 0.09, 0.09, 0.12, 0.09, 0.105, 0.105]
        t = ax_tbl.table(
            cellText=disp.values,
            colLabels=disp.columns,
            cellLoc="center",
            colLoc="center",
            colWidths=col_widths,
            bbox=[0.14, 0.14, 0.72, 0.72],
        )
        t.auto_set_font_size(False)
        t.set_fontsize(8.2)
        t.scale(1.0, 1.72)

        for (r, c), cell in t.get_celld().items():
            cell.set_edgecolor("#1F2937")
            cell.set_linewidth(0.7)
            if r == 0:
                cell.set_facecolor("#E5E7EB")
                cell.get_text().set_fontweight("bold")
                cell.get_text().set_fontsize(7.6)
                cell.set_height(cell.get_height() * 1.18)
            elif c in [5, 6]:
                cell.set_facecolor("#EEF5E9")
            else:
                cell.set_facecolor("#FFFFFF")

        # Color-code growth/fall cells.
        for ridx in range(1, len(disp) + 1):
            for cidx, raw_col in [(7, "var_vs_py_pct"), (8, "var_vs_lw_pct")]:
                v = table.iloc[ridx - 1][raw_col]
                tc = t[(ridx, cidx)].get_text()
                if pd.isna(v):
                    tc.set_color("#6B7280")
                elif float(v) > 0.05:
                    tc.set_color("#166534")
                elif float(v) < -0.05:
                    tc.set_color("#991B1B")
                else:
                    tc.set_color("#374151")

        # Replace brand text with logos where available.
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        for ridx in range(1, len(disp) + 1):
            brand = str(table.iloc[ridx - 1]["brand"])
            cell = t[(ridx, 0)]
            path = _logo_path(brand)
            if not path:
                cell.get_text().set_fontweight("bold")
                continue
            try:
                cell.get_text().set_text("")
                img = plt.imread(path)
                bbox = cell.get_window_extent(renderer=renderer)
                (x0, y0), (x1, y1) = ax_tbl.transAxes.inverted().transform([[bbox.x0, bbox.y0], [bbox.x1, bbox.y1]])
                w = max((x1 - x0) * 0.86, 0.001)
                h = max((y1 - y0) * 0.70, 0.001)
                lx = x0 + (x1 - x0 - w) / 2
                ly = y0 + (y1 - y0 - h) / 2
                lax = ax_tbl.inset_axes([lx, ly, w, h], transform=ax_tbl.transAxes, zorder=5)
                lax.imshow(img)
                lax.set_aspect("auto")
                lax.axis("off")
            except Exception:
                cell.get_text().set_text(brand)
                cell.get_text().set_fontweight("bold")

    ax_ts = fig.add_subplot(gs[1, 0])
    if series_df.empty:
        ax_ts.axis("off")
        ax_ts.text(0.5, 0.5, "No weekly series available.", ha="center", va="center")
        return

    series_df = series_df.copy()
    series_df["snapshot_date"] = pd.to_datetime(series_df["snapshot_date"], errors="coerce")
    valid_dates = sorted(series_df["snapshot_date"].dropna().unique().tolist())
    if not valid_dates:
        ax_ts.axis("off")
        ax_ts.text(0.5, 0.5, "No weekly series available.", ha="center", va="center")
        return

    # Dynamic rolling window: keep up to last 60 observations.
    keep_dates = set(valid_dates[-60:])
    plot_df = series_df[series_df["snapshot_date"].isin(keep_dates)].copy()

    plot_colors = {
        "Pirelli": "#0072B2",
        "Metzeler": "#009E73",
        "Michelin": "#CC79A7",
        "Continental": "#56B4E9",
        "Bridgestone": "#D55E00",
        "Dunlop": "#000000",
    }

    for row in table.itertuples(index=False):
        if str(row.pattern_set) == "-" or pd.isna(row.pattern_set):
            continue
        s = plot_df[(plot_df["brand"] == row.brand) & (plot_df["pattern_set"] == row.pattern_set)].sort_values("snapshot_date")
        if s.empty:
            continue
        ax_ts.plot(
            s["snapshot_date"],
            s["price"],
            marker="o",
            linewidth=2.0,
            label=str(row.brand),
            color=plot_colors.get(str(row.brand), _brand_color(str(row.brand))),
            alpha=0.95,
        )

    vals = plot_df["price"].dropna().to_numpy()
    if len(vals) > 0:
        ymin, ymax = float(np.min(vals)), float(np.max(vals))
        span = max(ymax - ymin, 1.0)
        pad = span * 0.14
        ax_ts.set_ylim(ymin - pad, ymax + pad)

    x_vals = [pd.Timestamp(x) for x in sorted(keep_dates)]
    if x_vals:
        step = max(1, len(x_vals) // 12)
        tick_vals = x_vals[::step]
        if x_vals[-1] not in tick_vals:
            tick_vals.append(x_vals[-1])
        ax_ts.set_xticks(tick_vals)
        ax_ts.set_xticklabels([f"W{int(x.isocalendar().week):02d}" for x in tick_vals], fontsize=9)

    ax_ts.set_title("Weekly Price Evolution by Brand (selected pattern set)", fontsize=11, fontweight="bold")
    _add_subplot_note(ax_ts, "X-axis shows ISO week numbers. Up to last 60 weekly observations are displayed.")
    ax_ts.set_ylabel("Median Price (PLN)")
    ax_ts.yaxis.set_major_locator(MaxNLocator(nbins=7))
    ax_ts.grid(axis="y", alpha=0.30)
    ax_ts.legend(frameon=False, ncol=3, fontsize=8, loc="best")



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


def _focused_segment_groups(max_groups: int = 10) -> list[str]:
    """Return focused groups in fixed Italian-style narrative order."""
    preferred_order = [
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

    try:
        mapping = load_canonical_mapping()
        groups = (
            mapping["segment_reference_group"]
            .astype("string")
            .fillna("")
            .str.strip()
            .tolist()
        )
    except Exception:
        groups = []

    seen: set[str] = set()
    available: list[str] = []
    for g in groups:
        if g and g not in seen:
            seen.add(g)
            available.append(g)

    out: list[str] = [g for g in preferred_order if g in available]

    # Append any non-standard groups deterministically by numeric code and line rank.
    leftovers = [g for g in available if g not in out]

    def _sort_key(g: str) -> tuple[int, int, str]:
        m_code = re.match(r"\s*(\d+)", g)
        code = int(m_code.group(1)) if m_code else 9999
        gl = g.upper()
        if " 1ST" in gl:
            line = 1
        elif " 2ND" in gl:
            line = 2
        elif " 3RD" in gl:
            line = 3
        else:
            line = 9
        return (code, line, g)

    out.extend(sorted(leftovers, key=_sort_key))

    if not out:
        out = ["706 - SUPERSPORT 1st"]

    return out[:max_groups]


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

        focused_groups = _focused_segment_groups(max_groups=10)
        primary_group = "706 - SUPERSPORT 1st"
        if primary_group not in focused_groups:
            primary_group = focused_groups[0] if focused_groups else "706 - SUPERSPORT 1st"

        # Page 3: Segment-level checkpoint (first focused group, default Supersport 1st)
        fig = plt.figure(figsize=(16, 9))
        _draw_segment_pattern_checkpoint_page(
            fig,
            silver=silver,
            latest=latest,
            prev=prev,
            segment_reference_group=primary_group,
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        if primary_group not in focused_groups:
            primary_group = focused_groups[0] if focused_groups else "706 - SUPERSPORT 1st"

        # Page 4..12: Remaining focused segment groups in same template as page 3.
        remaining_groups = [g for g in focused_groups if g != primary_group][:9]
        for group in remaining_groups:
            fig = plt.figure(figsize=(16, 9))
            _draw_segment_pattern_checkpoint_page(
                fig,
                silver=silver,
                latest=latest,
                prev=prev,
                segment_reference_group=group,
            )
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

    logger.info("PDF report written: %s", output)
    return output
