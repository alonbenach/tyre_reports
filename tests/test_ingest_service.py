from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from moto_app.ingest import remove_staged_intake_file
from moto_app.ingest.service import _count_rows
from moto_app.observability import OperatorFacingError


ROOT = Path(__file__).resolve().parents[1]


def test_remove_staged_intake_file_deletes_selected_snapshot() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    intake_dir = tmp_root / "ingest"
    intake_dir.mkdir(parents=True, exist_ok=True)
    staged_file = intake_dir / "2026-03-17.csv"
    staged_file.write_text("col\nvalue\n", encoding="utf-8")
    try:
        removed = remove_staged_intake_file(intake_dir, "2026-03-17")
        assert removed == staged_file
        assert not staged_file.exists()
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_remove_staged_intake_file_raises_for_missing_snapshot() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    intake_dir = tmp_root / "ingest"
    intake_dir.mkdir(parents=True, exist_ok=True)
    try:
        with pytest.raises(OperatorFacingError):
            remove_staged_intake_file(intake_dir, "2026-03-17")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_count_rows_handles_oversized_csv_fields() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    source_file = tmp_root / "2026-04-01.csv"
    large_cell = "x" * 200000
    source_text = (
        "product_code;EAN;price;price â‚¬;amount;realizationTime;productionYear;"
        "seller;actualization;is_retreaded;producer;size;width;rim;profil;"
        "speed;capacity;season;ROF;XL;name;type;date\n"
        f"SKU-1;111;100;25;5;24h;2026;Dealer;2026-04-01;0;Brand;120/70;120;17;70;"
        f"H;58;summer;0;0;{large_cell};Motocykle;2026-04-01\n"
        "SKU-2;222;110;27;2;24h;2026;Dealer;2026-04-01;0;Brand;190/50;190;17;50;"
        "W;73;summer;0;1;Short name;Osobowe;2026-04-01\n"
    )

    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        source_file.write_text(source_text, encoding="utf-8")

        assert _count_rows(source_file) == (2, 1)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
