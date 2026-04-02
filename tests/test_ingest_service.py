from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from moto_app.ingest import remove_staged_intake_file, scan_weekly_csv
from moto_app.ingest.service import INPUT_COLUMNS, _read_motorcycle_rows
from moto_app.observability import OperatorFacingError


ROOT = Path(__file__).resolve().parents[1]
HEADER = ";".join(INPUT_COLUMNS)


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


def test_scan_weekly_csv_handles_oversized_csv_fields() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    source_file = tmp_root / "2026-04-01.csv"
    large_cell = "x" * 200000
    source_text = (
        f"{HEADER}\n"
        f"SKU-1;111;100;25;5;24h;2026;Dealer;2026-04-01;0;Brand;120/70;120;17;70;"
        f"H;58;summer;0;0;{large_cell};Motocykle;2026-04-01\n"
        "SKU-2;222;110;27;2;24h;2026;Dealer;2026-04-01;0;Brand;190/50;190;17;50;"
        "W;73;summer;0;1;Short name;Osobowe;2026-04-01\n"
    )

    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        source_file.write_text(source_text, encoding="utf-8")

        scan_result = scan_weekly_csv(source_file)

        assert scan_result.snapshot_date == "2026-04-01"
        assert scan_result.row_count_total == 2
        assert scan_result.row_count_motorcycle == 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_read_motorcycle_rows_raises_operator_error_for_unclosed_quote() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    source_file = tmp_root / "2026-04-01.csv"
    malformed_csv = (
        f"{HEADER}\n"
        'SKU-1;111;100;25;5;24h;2026;Dealer;2026-04-01;0;Brand;120/70;120;17;70;'
        'H;58;summer;0;0;"broken name;Motocykle;2026-04-01\n'
    )

    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        source_file.write_text(malformed_csv, encoding="utf-8")

        with pytest.raises(OperatorFacingError, match="unclosed quoted value"):
            _read_motorcycle_rows(source_file, "2026-04-01")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def test_scan_weekly_csv_uses_signature_date() -> None:
    source_file = ROOT / "tests" / "fixtures" / "2026-02-10_sample.csv"

    scan_result = scan_weekly_csv(source_file)

    assert scan_result.snapshot_date == "2026-02-09"
    assert scan_result.row_count_total > 0
    assert scan_result.row_count_motorcycle > 0


def test_scan_weekly_csv_rejects_mixed_signature_dates() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    source_file = tmp_root / "mixed-signature-dates.csv"
    mixed_csv = (
        f"{HEADER}\n"
        "SKU-1;111;100;25;5;24h;2026;Dealer;01.04.2026 08:00;0;Brand;120/70;120;17;70;"
        "H;58;summer;0;0;Item A;Motocykle;01.04.2026\n"
        "SKU-2;222;110;27;2;24h;2026;Dealer;02.04.2026 09:15;0;Brand;190/50;190;17;50;"
        "W;73;summer;0;1;Item B;Motocykle;02.04.2026\n"
    )

    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        source_file.write_text(mixed_csv, encoding="utf-8")

        with pytest.raises(OperatorFacingError, match="multiple signature dates"):
            scan_weekly_csv(source_file)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
