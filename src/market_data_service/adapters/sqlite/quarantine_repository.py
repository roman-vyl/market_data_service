"""Durable storage of rare ingestion problems."""

from __future__ import annotations

import sqlite3

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.domain.identity import StreamKey


class SqliteQuarantineRepository:
    def __init__(self, connection: sqlite3.Connection, catalog: SqliteCatalogRepository) -> None:
        self._connection = connection
        self._catalog = catalog

    def add(
        self,
        *,
        stream: StreamKey,
        start_ms: int,
        end_ms: int,
        reason_code: str,
        detail: str,
        payload_json: str | None,
        created_at_ms: int,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO quarantine(
                stream_id, start_ms, end_ms, reason_code, detail, payload_json, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._catalog.stream_id(stream),
                start_ms,
                end_ms,
                reason_code,
                detail,
                payload_json,
                created_at_ms,
            ),
        )

    def contains(
        self,
        *,
        stream: StreamKey,
        start_ms: int,
        end_ms: int,
        reason_code: str,
    ) -> bool:
        row = self._connection.execute(
            """
            SELECT 1
            FROM quarantine
            WHERE stream_id = ? AND start_ms = ? AND end_ms = ? AND reason_code = ?
            LIMIT 1
            """,
            (self._catalog.stream_id(stream), start_ms, end_ms, reason_code),
        ).fetchone()
        return row is not None
