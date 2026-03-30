from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path

from moto_app.config import default_config


ROOT = Path(__file__).resolve().parents[1]


def test_packaged_prod_collapses_to_flat_root(monkeypatch) -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    tmp_root.mkdir(parents=True, exist_ok=True)
    exe_path = tmp_root / "MotoWeeklyOperator.exe"
    exe_path.write_text("", encoding="utf-8")
    try:
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "executable", str(exe_path))
        config = default_config(environment="prod")
        assert config.app_root == tmp_root
        assert config.database_path == tmp_root / "database" / "moto_pipeline.db"
        assert config.reports_dir == tmp_root / "reports"
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
