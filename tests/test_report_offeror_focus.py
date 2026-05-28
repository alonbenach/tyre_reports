from __future__ import annotations

import pandas as pd

from moto_pipeline.report_offeror_focus import _build_page1_table, _draw_pdf_table


class _DummyAxis:
    transAxes = object()

    def text(self, *args, **kwargs) -> None:
        return None

    def plot(self, *args, **kwargs) -> None:
        return None


def test_draw_pdf_table_accepts_status_only_layout() -> None:
    display = pd.DataFrame([{"Status": "No data available for page 1."}])
    source = display.copy()

    _draw_pdf_table(
        ax=_DummyAxis(),
        display=display,
        source=source,
        bbox=[0.01, 0.05, 0.98, 0.85],
        col_widths=[1.0],
    )


def test_page1_selects_price_setter_by_lowest_seller_median() -> None:
    latest = pd.Timestamp("2026-04-08")
    group = "706 - SUPERSPORT 1st"
    brand = "Pirelli"
    pattern = "Diablo Rosso IV"
    size_root = "120/70 17"

    silver = pd.DataFrame(
        [
            {
                "snapshot_date": latest,
                "brand": brand,
                "is_high_confidence_match": True,
                "segment_reference_group": group,
                "pattern_set": pattern,
                "size_root": size_root,
                "seller_norm": "High Stock Seller",
                "stock_qty": 100,
                "price_pln": 500,
                "list_price": 1000,
                "is_extra_3pct_set": False,
                "discount_vs_list_implied": 0.5,
                "expected_net_price_from_list": 400,
                "ipcode": "IP1",
                "size_norm": size_root,
            },
            {
                "snapshot_date": latest,
                "brand": brand,
                "is_high_confidence_match": True,
                "segment_reference_group": group,
                "pattern_set": pattern,
                "size_root": size_root,
                "seller_norm": "High Stock Seller",
                "stock_qty": 80,
                "price_pln": 520,
                "list_price": 1000,
                "is_extra_3pct_set": False,
                "discount_vs_list_implied": 0.48,
                "expected_net_price_from_list": 400,
                "ipcode": "IP1",
                "size_norm": size_root,
            },
            {
                "snapshot_date": latest,
                "brand": brand,
                "is_high_confidence_match": True,
                "segment_reference_group": group,
                "pattern_set": pattern,
                "size_root": size_root,
                "seller_norm": "Price Setter",
                "stock_qty": 3,
                "price_pln": 450,
                "list_price": 1000,
                "is_extra_3pct_set": False,
                "discount_vs_list_implied": 0.55,
                "expected_net_price_from_list": 400,
                "ipcode": "IP2",
                "size_norm": size_root,
            },
            {
                "snapshot_date": latest,
                "brand": brand,
                "is_high_confidence_match": True,
                "segment_reference_group": group,
                "pattern_set": pattern,
                "size_root": size_root,
                "seller_norm": "Price Setter",
                "stock_qty": 2,
                "price_pln": 470,
                "list_price": 1000,
                "is_extra_3pct_set": False,
                "discount_vs_list_implied": 0.53,
                "expected_net_price_from_list": 400,
                "ipcode": "IP2",
                "size_norm": size_root,
            },
        ]
    )
    mapping = pd.DataFrame(
        [
            {
                "segment_reference_group": group,
                "brand": brand,
                "key_fitments": size_root,
            }
        ]
    )

    table = _build_page1_table(
        silver=silver,
        latest=latest,
        prev=None,
        canonical_mapping=mapping,
        customer_discounts=pd.DataFrame(),
    )

    row = table.iloc[0]
    assert row["first_player"] == "Price Setter"
    assert row["stock"] == 5
    assert row["platforma_price"] == 460
