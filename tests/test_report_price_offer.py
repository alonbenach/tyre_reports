from __future__ import annotations

from pathlib import Path

import pandas as pd

from moto_pipeline.report_price_offer import _read_recap_latest


def test_read_recap_latest_builds_week_label_without_arrow_string_arithmetic(
) -> None:
    gold_dir = Path("tests/.tmp/report_price_offer/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)
    for path in gold_dir.glob("*"):
        path.unlink()

    recap = pd.DataFrame(
        {
            "snapshot_date": ["2026-02-10"],
            "brand": ["Pirelli"],
            "positioning_index_round": [100],
            "vs_prev_week_round": [1],
            "vs_py_round": [-2],
        }
    )
    (gold_dir / "gold_recap_by_brand_weekly.csv").write_text(
        recap.to_csv(index=False),
        encoding="utf-8",
    )

    result = _read_recap_latest(gold_dir)

    assert result.loc[0, "week_label"] == "2026-W07"
    assert result.loc[0, "positioning_display"] == "100"
    assert result.loc[0, "vs_prev_week_display"] == "+1"
    assert result.loc[0, "vs_py_display"] == "-2"
