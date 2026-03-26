from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from moto_app.ingest import remove_staged_intake_file
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
