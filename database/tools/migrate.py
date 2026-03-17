from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from moto_app.db.runtime import connect_sqlite


@dataclass(frozen=True)
class DatabasePaths:
    """Filesystem locations required for DB bootstrap."""

    db_path: Path
    migrations_dir: Path


@dataclass(frozen=True)
class MigrationResult:
    """Outcome of one initialization run."""

    db_path: Path
    applied_versions: list[str]
    skipped_versions: list[str]


def _list_migration_files(migrations_dir: Path) -> list[Path]:
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No migration files found in {migrations_dir}")
    return files


def _ensure_schema_migrations_table(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL
        );
        """
    )


def _get_applied_versions(connection) -> set[str]:
    _ensure_schema_migrations_table(connection)
    rows = connection.execute("SELECT version FROM schema_migrations;").fetchall()
    return {str(row["version"]) for row in rows}


def _apply_migration(connection, version: str, sql_text: str) -> None:
    script = f"""
BEGIN IMMEDIATE;
{sql_text}
INSERT INTO schema_migrations (version, applied_at_utc)
VALUES ('{version}', datetime('now'));
COMMIT;
"""
    try:
        connection.executescript(script)
    except Exception:
        connection.rollback()
        raise


def initialize_database(paths: DatabasePaths) -> MigrationResult:
    """Create the DB if needed and apply ordered SQL migrations."""

    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    migration_files = _list_migration_files(paths.migrations_dir)
    applied_versions: list[str] = []
    skipped_versions: list[str] = []

    with connect_sqlite(paths.db_path) as connection:
        _ensure_schema_migrations_table(connection)
        existing = _get_applied_versions(connection)

        for migration_file in migration_files:
            version = migration_file.stem
            if version in existing:
                skipped_versions.append(version)
                continue

            sql_text = migration_file.read_text(encoding="utf-8")
            _apply_migration(connection, version, sql_text)
            applied_versions.append(version)

    return MigrationResult(
        db_path=paths.db_path,
        applied_versions=applied_versions,
        skipped_versions=skipped_versions,
    )
