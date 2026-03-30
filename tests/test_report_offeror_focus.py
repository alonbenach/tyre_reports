from __future__ import annotations

import pandas as pd

from moto_pipeline.report_offeror_focus import _draw_pdf_table


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
