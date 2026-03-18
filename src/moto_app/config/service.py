from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    app_root: Path
    data_dir: Path
    intake_dir: Path
    raw_archive_dir: Path
    database_dir: Path
    database_path: Path
    reports_dir: Path
    logs_dir: Path
    assets_dir: Path
    reference_source_dir: Path
    default_report_mode: str
    include_pdf_by_default: bool
    runtime_mode: str


def _default_root() -> Path:
    return Path(__file__).resolve().parents[3]


def detect_runtime_mode() -> str:
    return "packaged" if getattr(sys, "frozen", False) else "development"


def default_config(app_root: Path | None = None) -> AppConfig:
    root = Path(app_root) if app_root is not None else _default_root()
    database_dir = root / "database"
    return AppConfig(
        app_root=root,
        data_dir=root / "data",
        intake_dir=root / "data" / "ingest",
        raw_archive_dir=root / "data" / "raw",
        database_dir=database_dir,
        database_path=database_dir / "moto_pipeline.db",
        reports_dir=root / "reports",
        logs_dir=root / "logs",
        assets_dir=root / "assets",
        reference_source_dir=root / "data" / "campaign rules",
        default_report_mode="excel",
        include_pdf_by_default=False,
        runtime_mode=detect_runtime_mode(),
    )


def ensure_runtime_dirs(config: AppConfig) -> None:
    for path in [
        config.data_dir,
        config.intake_dir,
        config.raw_archive_dir,
        config.database_dir,
        config.reports_dir,
        config.logs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def load_config(
    app_root: Path | None = None,
    *,
    config_override_path: Path | None = None,
) -> AppConfig:
    config = default_config(app_root)
    if config_override_path is None or not config_override_path.exists():
        return config

    payload = json.loads(config_override_path.read_text(encoding="utf-8"))
    merged = asdict(config)
    merged.update(payload)
    for key, value in list(merged.items()):
        if key.endswith("_dir") or key.endswith("_path") or key == "app_root":
            merged[key] = Path(value)
    return AppConfig(**merged)
