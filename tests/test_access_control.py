from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
import shutil
import uuid

from moto_app.access_control import (
    acquire_access_session,
    evaluate_access,
    refresh_access_heartbeat,
    release_access_session,
)
from moto_app.config import default_config


ROOT = Path(__file__).resolve().parents[1]


def _config_for(tmp_path: Path):
    base = default_config(ROOT)
    return replace(
        base,
        session_lock_path=tmp_path / "session.lock",
        admin_users=("admin-user",),
        runtime_mode="production",
        lock_heartbeat_seconds=15,
        lock_stale_seconds=300,
    )


def test_access_control_allows_first_writable_session(monkeypatch) -> None:
    tmp_path = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = _config_for(tmp_path)
    monkeypatch.setattr("moto_app.access_control.service.current_identity", lambda: ("operator-a", "machine-a"))
    try:
        evaluation = evaluate_access(config)
        assert evaluation.mode == "writable"

        session = acquire_access_session(config)
        assert session.mode == "writable"
        assert config.session_lock_path.exists()

        refreshed = refresh_access_heartbeat(config, session)
        assert refreshed.active_lock is not None

        release_access_session(config, refreshed)
        assert not config.session_lock_path.exists()
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_access_control_opens_second_session_read_only(monkeypatch) -> None:
    tmp_path = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = _config_for(tmp_path)
    try:
        monkeypatch.setattr("moto_app.access_control.service.current_identity", lambda: ("operator-a", "machine-a"))
        first = acquire_access_session(config)
        assert first.mode == "writable"

        monkeypatch.setattr("moto_app.access_control.service.current_identity", lambda: ("operator-b", "machine-b"))
        evaluation = evaluate_access(config)
        assert evaluation.mode == "read_only"
        assert not evaluation.is_lock_stale
        assert evaluation.active_lock is not None
        assert evaluation.active_lock.user_name == "operator-a"

        second = acquire_access_session(config)
        assert second.mode == "read_only"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_admin_user_can_recover_stale_lock(monkeypatch) -> None:
    tmp_path = ROOT / "database" / "_test_work" / str(uuid.uuid4())
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = _config_for(tmp_path)
    try:
        monkeypatch.setattr("moto_app.access_control.service.current_identity", lambda: ("operator-a", "machine-a"))
        session = acquire_access_session(config)
        assert session.mode == "writable"

        stale_time = datetime.now(UTC) - timedelta(seconds=config.lock_stale_seconds + 10)
        monkeypatch.setattr("moto_app.access_control.service._utc_now", lambda: stale_time)
        stale_session = refresh_access_heartbeat(config, session)
        assert stale_session.active_lock is not None

        monkeypatch.setattr("moto_app.access_control.service.current_identity", lambda: ("admin-user", "machine-admin"))
        now = datetime.now(UTC)
        monkeypatch.setattr("moto_app.access_control.service._utc_now", lambda: now)
        evaluation = evaluate_access(config)
        assert evaluation.mode == "read_only"
        assert evaluation.is_lock_stale
        assert evaluation.can_recover_stale_lock

        recovered = acquire_access_session(config, recover_stale_lock=True)
        assert recovered.mode == "writable"
        assert recovered.reason == "Recovered a stale writable session lock."
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
