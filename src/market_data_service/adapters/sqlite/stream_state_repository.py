"""Per-stream lifecycle snapshot persistence."""

from __future__ import annotations

import sqlite3

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState, StreamStateSnapshot


class SqliteStreamStateRepository:
    def __init__(self, connection: sqlite3.Connection, catalog: SqliteCatalogRepository) -> None:
        self._connection = connection
        self._catalog = catalog

    def get(self, stream: StreamKey) -> StreamStateSnapshot:
        row = self._connection.execute(
            "SELECT * FROM stream_state WHERE stream_id = ?",
            (self._catalog.stream_id(stream),),
        ).fetchone()
        if row is None:
            raise KeyError(stream.canonical_id)
        return StreamStateSnapshot(
            stream=stream,
            state=StreamLifecycleState(row["state"]),
            earliest_available_open_time_ms=row["earliest_available_open_time_ms"],
            latest_committed_open_time_ms=row["latest_committed_open_time_ms"],
            last_audit_at_ms=row["last_audit_at_ms"],
            last_rest_success_at_ms=row["last_rest_success_at_ms"],
            last_ws_message_at_ms=row["last_ws_message_at_ms"],
            last_error_code=row["last_error_code"],
            last_error_detail=row["last_error_detail"],
            state_changed_at_ms=row["state_changed_at_ms"],
            updated_at_ms=row["updated_at_ms"],
        )

    def save(self, snapshot: StreamStateSnapshot) -> None:
        self._connection.execute(
            """
            UPDATE stream_state SET
                state = ?, earliest_available_open_time_ms = ?,
                latest_committed_open_time_ms = ?, last_audit_at_ms = ?,
                last_rest_success_at_ms = ?, last_ws_message_at_ms = ?,
                last_error_code = ?, last_error_detail = ?,
                state_changed_at_ms = ?, updated_at_ms = ?
            WHERE stream_id = ?
            """,
            (
                snapshot.state.value,
                snapshot.earliest_available_open_time_ms,
                snapshot.latest_committed_open_time_ms,
                snapshot.last_audit_at_ms,
                snapshot.last_rest_success_at_ms,
                snapshot.last_ws_message_at_ms,
                snapshot.last_error_code,
                snapshot.last_error_detail,
                snapshot.state_changed_at_ms,
                snapshot.updated_at_ms,
                self._catalog.stream_id(snapshot.stream),
            ),
        )
