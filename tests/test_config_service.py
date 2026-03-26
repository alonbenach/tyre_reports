from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from moto_app.config import default_config, ensure_runtime_dirs


ROOT = Path(__file__).resolve().parents[1]


def test_default_config_dev_uses_repo_root() -> None:
    config = default_config(ROOT, environment="dev")
    assert config.environment_name == "dev"
    assert config.app_root == ROOT
    assert config.database_path == ROOT / "database" / "moto_pipeline.db"
    assert config.intake_dir == ROOT / "data" / "ingest"


def test_default_config_prod_uses_runtime_prod_root() -> None:
    tmp_root = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    tmp_root.mkdir(parents=True, exist_ok=True)
    try:
        config = default_config(tmp_root, environment="prod")
        expected_root = tmp_root / "runtime" / "prod"
        assert config.environment_name == "prod"
        assert config.runtime_mode == "production"
        assert config.app_root == expected_root
        assert config.database_path == expected_root / "database" / "moto_pipeline.db"
        assert config.reports_dir == expected_root / "reports"
        ensure_runtime_dirs(config)
        assert config.database_dir.exists()
        assert config.intake_dir.exists()
        assert config.logs_dir.exists()
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
