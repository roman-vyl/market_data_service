"""Open interest values and track progress persistence."""

from __future__ import annotations

import sqlite3

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import CanonicalOpenInterest, TrackProgress

_SOURCE = "bybit_rest"


class SqliteOpenInterestRepository:
    def __init__(self, connection: sqlite3.Connection, catalog: SqliteCatalogRepository) -> None:
        self._connection = connection
        self._catalog = catalog

    def get(self, stream: StreamKey, open_time_ms: int) -> CanonicalOpenInterest | None:
        row = self._connection.execute(
            "SELECT * FROM open_interest WHERE stream_id = ? AND open_time_ms = ?",
            (self._catalog.stream_id(stream), open_time_ms),
        ).fetchone()
        if row is None:
            return None
        return CanonicalOpenInterest(
            stream=stream,
            open_time_ms=row["open_time_ms"],
            open_interest=row["open_interest_value"],
            committed_at_ms=row["committed_at_ms"],
        )

    def save(self, point: CanonicalOpenInterest) -> None:
        self._connection.execute(
            """
            INSERT INTO open_interest (
                stream_id, open_time_ms, open_interest_value, source, committed_at_ms
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, open_time_ms) DO UPDATE SET
                open_interest_value = excluded.open_interest_value,
                source = excluded.source,
                committed_at_ms = excluded.committed_at_ms
            """,
            (
                self._catalog.stream_id(point.stream),
                point.open_time_ms,
                point.open_interest,
                _SOURCE,
                point.committed_at_ms,
            ),
        )

    def list_times(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[int, ...]:
        rows = self._connection.execute(
            """
            SELECT open_time_ms FROM open_interest
            WHERE stream_id = ? AND open_time_ms >= ? AND open_time_ms < ?
            ORDER BY open_time_ms
            """,
            (self._catalog.stream_id(stream), start_time_ms, end_time_ms),
        ).fetchall()
        return tuple(int(row["open_time_ms"]) for row in rows)


class SqliteTrackProgressRepository:
    def __init__(self, connection: sqlite3.Connection, catalog: SqliteCatalogRepository) -> None:
        self._connection = connection
        self._catalog = catalog

    def get(self, stream: StreamKey, track: str) -> TrackProgress:
        row = self._connection.execute(
            "SELECT * FROM track_progress WHERE stream_id = ? AND track = ?",
            (self._catalog.stream_id(stream), track),
        ).fetchone()
        if row is None:
            return TrackProgress(stream=stream, track=track)
        return TrackProgress(
            stream=stream,
            track=track,
            earliest_available_open_time_ms=row["earliest_available_open_time_ms"],
            discovery_next_open_time_ms=row["discovery_next_open_time_ms"],
            latest_committed_open_time_ms=row["latest_committed_open_time_ms"],
            last_audit_at_ms=row["last_audit_at_ms"],
            last_audit_gap_count=row["last_audit_gap_count"],
            last_success_at_ms=row["last_success_at_ms"],
            last_error_code=row["last_error_code"],
            last_error_detail=row["last_error_detail"],
            updated_at_ms=row["updated_at_ms"],
        )

    def save(self, progress: TrackProgress) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO track_progress (
                stream_id, track, earliest_available_open_time_ms,
                discovery_next_open_time_ms, latest_committed_open_time_ms,
                last_audit_at_ms, last_audit_gap_count, last_success_at_ms,
                last_error_code, last_error_detail, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._catalog.stream_id(progress.stream),
                progress.track,
                progress.earliest_available_open_time_ms,
                progress.discovery_next_open_time_ms,
                progress.latest_committed_open_time_ms,
                progress.last_audit_at_ms,
                progress.last_audit_gap_count,
                progress.last_success_at_ms,
                progress.last_error_code,
                progress.last_error_detail,
                progress.updated_at_ms,
            ),
        )
