from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def connect_sqlite(db_path: Path):
    """Open a SQLite connection with conservative defaults."""

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA journal_mode = WAL;")
    try:
        yield connection
    finally:
        connection.close()
