from __future__ import annotations

import getpass
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    environment_name: str
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
    session_lock_path: Path
    default_report_mode: str
    include_pdf_by_default: bool
    runtime_mode: str
    admin_users: tuple[str, ...]
    lock_heartbeat_seconds: int
    lock_stale_seconds: int


def _default_root() -> Path:
    return Path(__file__).resolve().parents[3]


def detect_runtime_mode() -> str:
    return "packaged" if getattr(sys, "frozen", False) else "development"


def _runtime_root(base_root: Path, environment: str) -> Path:
    if environment == "dev":
        return base_root
    if environment == "prod":
        return base_root / "runtime" / "prod"
    raise ValueError(f"Unsupported environment: {environment}")


def default_config(app_root: Path | None = None, *, environment: str = "dev") -> AppConfig:
    base_root = Path(app_root) if app_root is not None else _default_root()
    root = _runtime_root(base_root, environment)
    database_dir = root / "database"
    runtime_mode = detect_runtime_mode() if environment == "dev" else "production"
    return AppConfig(
        environment_name=environment,
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
        session_lock_path=database_dir / "session.lock",
        default_report_mode="excel",
        include_pdf_by_default=False,
        runtime_mode=runtime_mode,
        admin_users=(getpass.getuser(),),
        lock_heartbeat_seconds=15,
        lock_stale_seconds=300,
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
    environment: str = "dev",
    config_override_path: Path | None = None,
) -> AppConfig:
    config = default_config(app_root, environment=environment)
    if config_override_path is None or not config_override_path.exists():
        return config

    payload = json.loads(config_override_path.read_text(encoding="utf-8"))
    merged = asdict(config)
    merged.update(payload)
    for key, value in list(merged.items()):
        if key.endswith("_dir") or key.endswith("_path") or key == "app_root":
            merged[key] = Path(value)
    return AppConfig(**merged)
