"""SQLite schema creation and version validation."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "2"
_SCHEMA_PATH = Path(__file__).with_name("schema_v2.sql")


class UnsupportedSchemaVersion(RuntimeError):
    pass


def create_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
    connection.commit()


def migrate_schema(connection: sqlite3.Connection) -> None:
    """Apply supported forward-only migrations without dual schema reads."""

    row = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    actual = None if row is None else str(row["value"])
    if actual == SCHEMA_VERSION:
        return
    if actual != "1":
        raise UnsupportedSchemaVersion(f"expected schema 1 or {SCHEMA_VERSION}, found {actual}")

    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            CREATE TABLE stream_state_v2 (
                stream_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                earliest_available_open_time_ms INTEGER,
                lower_bound_discovery_next_open_time_ms INTEGER,
                latest_committed_open_time_ms INTEGER,
                last_audit_at_ms INTEGER,
                last_rest_success_at_ms INTEGER,
                last_ws_message_at_ms INTEGER,
                last_error_code TEXT,
                last_error_detail TEXT,
                state_changed_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
                CHECK (state IN (
                    'uninitialized',
                    'bootstrapping',
                    'auditing',
                    'repairing',
                    'connecting',
                    'ready',
                    'degraded',
                    'failed'
                )),
                CHECK (
                    lower_bound_discovery_next_open_time_ms IS NULL
                    OR lower_bound_discovery_next_open_time_ms >= 0
                ),
                CHECK (
                    earliest_available_open_time_ms IS NULL
                    OR lower_bound_discovery_next_open_time_ms IS NULL
                )
            )
            """
        )
        connection.execute(
            """
            INSERT INTO stream_state_v2 (
                stream_id,
                state,
                earliest_available_open_time_ms,
                latest_committed_open_time_ms,
                last_audit_at_ms,
                last_rest_success_at_ms,
                last_ws_message_at_ms,
                last_error_code,
                last_error_detail,
                state_changed_at_ms,
                updated_at_ms
            )
            SELECT
                stream_id,
                state,
                earliest_available_open_time_ms,
                latest_committed_open_time_ms,
                last_audit_at_ms,
                last_rest_success_at_ms,
                last_ws_message_at_ms,
                last_error_code,
                last_error_detail,
                state_changed_at_ms,
                updated_at_ms
            FROM stream_state
            """
        )
        connection.execute("DROP TABLE stream_state")
        connection.execute("ALTER TABLE stream_state_v2 RENAME TO stream_state")
        connection.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
            (SCHEMA_VERSION,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def validate_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None or row["value"] != SCHEMA_VERSION:
        actual = None if row is None else row["value"]
        raise UnsupportedSchemaVersion(f"expected schema {SCHEMA_VERSION}, found {actual}")
    required_tables = {
        "schema_meta",
        "instruments",
        "streams",
        "candles",
        "stream_state",
        "quarantine",
    }
    actual_tables = {
        str(item["name"])
        for item in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    missing_tables = required_tables - actual_tables
    if missing_tables:
        raise UnsupportedSchemaVersion(
            f"schema {SCHEMA_VERSION} missing tables: {sorted(missing_tables)}"
        )
    stream_state_columns = {
        str(item["name"])
        for item in connection.execute("PRAGMA table_info(stream_state)")
    }
    if "lower_bound_discovery_next_open_time_ms" not in stream_state_columns:
        raise UnsupportedSchemaVersion(
            "schema 2 missing lower_bound_discovery_next_open_time_ms"
        )
