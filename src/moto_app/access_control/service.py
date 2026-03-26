from __future__ import annotations

import getpass
import json
import socket
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from moto_app.config import AppConfig


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_text(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


@dataclass(frozen=True)
class LockMetadata:
    session_id: str
    user_name: str
    machine_name: str
    app_version: str
    session_mode: str
    acquired_at_utc: str
    last_heartbeat_utc: str


@dataclass(frozen=True)
class AccessEvaluation:
    mode: str
    reason: str
    is_admin_user: bool
    active_lock: LockMetadata | None
    is_lock_stale: bool
    can_recover_stale_lock: bool


@dataclass(frozen=True)
class AccessSession:
    mode: str
    reason: str
    is_admin_user: bool
    user_name: str
    machine_name: str
    lock_path: Path
    session_id: str | None = None
    active_lock: LockMetadata | None = None
    admin_mode_enabled: bool = False


class AccessControlError(RuntimeError):
    """Raised when an access-control action is not permitted."""


def current_identity() -> tuple[str, str]:
    return getpass.getuser(), socket.gethostname()


def _is_admin_user(config: AppConfig, user_name: str) -> bool:
    normalized = {user.strip().lower() for user in config.admin_users}
    return config.runtime_mode == "development" or user_name.strip().lower() in normalized


def _stale_cutoff(config: AppConfig, now: datetime) -> datetime:
    return now - timedelta(seconds=config.lock_stale_seconds)


def _load_lock(lock_path: Path) -> LockMetadata | None:
    if not lock_path.exists():
        return None
    payload = json.loads(lock_path.read_text(encoding="utf-8"))
    return LockMetadata(
        session_id=str(payload.get("session_id", "")),
        user_name=str(payload.get("user_name", "")),
        machine_name=str(payload.get("machine_name", "")),
        app_version=str(payload.get("app_version", "")),
        session_mode=str(payload.get("session_mode", "writable")),
        acquired_at_utc=str(payload.get("acquired_at_utc", "")),
        last_heartbeat_utc=str(payload.get("last_heartbeat_utc", "")),
    )


def _write_lock(lock_path: Path, metadata: LockMetadata) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "session_id": metadata.session_id,
        "user_name": metadata.user_name,
        "machine_name": metadata.machine_name,
        "app_version": metadata.app_version,
        "session_mode": metadata.session_mode,
        "acquired_at_utc": metadata.acquired_at_utc,
        "last_heartbeat_utc": metadata.last_heartbeat_utc,
    }
    lock_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _create_lock_exclusive(lock_path: Path, metadata: LockMetadata) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "session_id": metadata.session_id,
        "user_name": metadata.user_name,
        "machine_name": metadata.machine_name,
        "app_version": metadata.app_version,
        "session_mode": metadata.session_mode,
        "acquired_at_utc": metadata.acquired_at_utc,
        "last_heartbeat_utc": metadata.last_heartbeat_utc,
    }
    try:
        with lock_path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except FileExistsError:
        return False
    return True


def _lock_is_stale(config: AppConfig, metadata: LockMetadata | None, now: datetime) -> bool:
    if metadata is None:
        return False
    heartbeat = _parse_utc(metadata.last_heartbeat_utc) or _parse_utc(metadata.acquired_at_utc)
    if heartbeat is None:
        return True
    return heartbeat < _stale_cutoff(config, now)


def evaluate_access(config: AppConfig) -> AccessEvaluation:
    user_name, _machine_name = current_identity()
    is_admin_user = _is_admin_user(config, user_name)
    active_lock = _load_lock(config.session_lock_path)
    now = _utc_now()
    is_stale = _lock_is_stale(config, active_lock, now)

    if active_lock is None:
        return AccessEvaluation(
            mode="writable",
            reason="No active writable session lock exists.",
            is_admin_user=is_admin_user,
            active_lock=None,
            is_lock_stale=False,
            can_recover_stale_lock=False,
        )
    if is_stale:
        return AccessEvaluation(
            mode="read_only",
            reason="A stale writable session lock was found.",
            is_admin_user=is_admin_user,
            active_lock=active_lock,
            is_lock_stale=True,
            can_recover_stale_lock=is_admin_user,
        )
    return AccessEvaluation(
        mode="read_only",
        reason="Another writable operator session is active.",
        is_admin_user=is_admin_user,
        active_lock=active_lock,
        is_lock_stale=False,
        can_recover_stale_lock=False,
    )


def lock_owner_summary(lock: LockMetadata | None) -> str:
    if lock is None:
        return "No writable session lock is active."
    owner = f"{lock.user_name}@{lock.machine_name}"
    heartbeat = lock.last_heartbeat_utc or "unknown"
    return f"{owner} (last heartbeat: {heartbeat})"


def acquire_access_session(config: AppConfig, *, recover_stale_lock: bool = False) -> AccessSession:
    user_name, machine_name = current_identity()
    evaluation = evaluate_access(config)

    if evaluation.mode == "read_only" and not (evaluation.is_lock_stale and recover_stale_lock and evaluation.can_recover_stale_lock):
        return AccessSession(
            mode="read_only",
            reason=evaluation.reason,
            is_admin_user=evaluation.is_admin_user,
            user_name=user_name,
            machine_name=machine_name,
            lock_path=config.session_lock_path,
            active_lock=evaluation.active_lock,
            admin_mode_enabled=False,
        )

    session_id = str(uuid.uuid4())
    now_text = _utc_text(_utc_now())
    metadata = LockMetadata(
        session_id=session_id,
        user_name=user_name,
        machine_name=machine_name,
        app_version=config.runtime_mode,
        session_mode="writable",
        acquired_at_utc=now_text,
        last_heartbeat_utc=now_text,
    )
    if not (recover_stale_lock and evaluation.is_lock_stale):
        if not _create_lock_exclusive(config.session_lock_path, metadata):
            latest = evaluate_access(config)
            return AccessSession(
                mode="read_only",
                reason=latest.reason,
                is_admin_user=latest.is_admin_user,
                user_name=user_name,
                machine_name=machine_name,
                lock_path=config.session_lock_path,
                active_lock=latest.active_lock,
                admin_mode_enabled=False,
            )
    else:
        _write_lock(config.session_lock_path, metadata)
    reason = "Writable session lock acquired."
    if recover_stale_lock and evaluation.is_lock_stale:
        reason = "Recovered a stale writable session lock."
    return AccessSession(
        mode="writable",
        reason=reason,
        is_admin_user=evaluation.is_admin_user,
        user_name=user_name,
        machine_name=machine_name,
        lock_path=config.session_lock_path,
        session_id=session_id,
        active_lock=metadata,
    )


def refresh_access_heartbeat(config: AppConfig, session: AccessSession) -> AccessSession:
    if session.mode != "writable" or session.session_id is None:
        return session
    metadata = _load_lock(config.session_lock_path)
    if metadata is None or metadata.session_id != session.session_id:
        return session
    updated = LockMetadata(
        session_id=metadata.session_id,
        user_name=metadata.user_name,
        machine_name=metadata.machine_name,
        app_version=metadata.app_version,
        session_mode=metadata.session_mode,
        acquired_at_utc=metadata.acquired_at_utc,
        last_heartbeat_utc=_utc_text(_utc_now()),
    )
    _write_lock(config.session_lock_path, updated)
    return AccessSession(
        mode=session.mode,
        reason=session.reason,
        is_admin_user=session.is_admin_user,
        user_name=session.user_name,
        machine_name=session.machine_name,
        lock_path=session.lock_path,
        session_id=session.session_id,
        active_lock=updated,
        admin_mode_enabled=session.admin_mode_enabled,
    )


def enable_admin_mode(config: AppConfig, session: AccessSession) -> AccessSession:
    if not session.is_admin_user:
        raise AccessControlError("Admin controls are only available to configured admin users.")
    if session.mode != "writable":
        raise AccessControlError("Admin controls can only be enabled from the active writable session.")
    if session.admin_mode_enabled:
        return session
    return AccessSession(
        mode=session.mode,
        reason=session.reason,
        is_admin_user=session.is_admin_user,
        user_name=session.user_name,
        machine_name=session.machine_name,
        lock_path=session.lock_path,
        session_id=session.session_id,
        active_lock=session.active_lock,
        admin_mode_enabled=True,
    )


def recover_stale_lock_session(config: AppConfig, session: AccessSession) -> AccessSession:
    if not session.is_admin_user:
        raise AccessControlError("Only configured admin users may recover a stale lock.")
    latest = evaluate_access(config)
    if not latest.is_lock_stale:
        raise AccessControlError("The current writable-session lock is not stale, so it cannot be cleared.")
    if not latest.can_recover_stale_lock:
        raise AccessControlError("This session is not allowed to recover the stale lock.")
    recovered = acquire_access_session(config, recover_stale_lock=True)
    if recovered.mode != "writable":
        raise AccessControlError("Could not take ownership of the writable session after stale-lock recovery.")
    return AccessSession(
        mode=recovered.mode,
        reason=recovered.reason,
        is_admin_user=recovered.is_admin_user,
        user_name=recovered.user_name,
        machine_name=recovered.machine_name,
        lock_path=recovered.lock_path,
        session_id=recovered.session_id,
        active_lock=recovered.active_lock,
        admin_mode_enabled=True,
    )


def release_access_session(config: AppConfig, session: AccessSession) -> None:
    if session.mode != "writable" or session.session_id is None:
        return
    metadata = _load_lock(config.session_lock_path)
    if metadata is None:
        return
    if metadata.session_id != session.session_id:
        return
    config.session_lock_path.unlink(missing_ok=True)
