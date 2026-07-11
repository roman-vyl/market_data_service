"""SQLite schema creation and version validation."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "1"
_SCHEMA_PATH = Path(__file__).with_name("schema_v1.sql")


class UnsupportedSchemaVersion(RuntimeError):
    pass


def create_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
    connection.commit()


def validate_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None or row["value"] != SCHEMA_VERSION:
        actual = None if row is None else row["value"]
        raise UnsupportedSchemaVersion(f"expected schema {SCHEMA_VERSION}, found {actual}")
