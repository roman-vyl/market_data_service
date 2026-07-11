"""Support checks used by the local REST smoke runner."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

from market_data_service.adapters.bybit import (
    BybitApiError,
    BybitPayloadError,
    BybitRestCandleSource,
)
from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain import (
    ObservationSource,
    ObservedCandle,
    StreamKey,
    TimeWindow,
    get_timeframe,
)


class Clock(Protocol):
    def now_ms(self) -> int: ...


@dataclass(frozen=True, slots=True)
class PersistenceSnapshot:
    schema_version: str
    candles: int
    stream_state_rows: int
    latest_committed_open_time_ms: int | None
    quarantine_rows: int
    open_times_ms: tuple[int, ...] = ()


class StaticPayloadTransport:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def get_json(
        self,
        url: str,
        params: dict[str, str | int],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        return self._payload


def run_error_scenarios(
    database_path: Path,
    stream: StreamKey,
    window: TimeWindow,
    clock: Clock,
) -> bool:
    wrong_symbol_ok = _check_wrong_symbol(stream, window, clock)
    empty_payload_ok = _check_empty_payload(stream, window, clock)
    invalid_candle_ok = _check_invalid_candle(database_path, stream, window, clock)
    return wrong_symbol_ok and empty_payload_ok and invalid_candle_ok


def inspect_persistence(database_path: Path, stream: StreamKey) -> PersistenceSnapshot:
    """Inspect SQLite directly for smoke-only verification diagnostics.

    Production entrypoints should use repositories/use cases instead of copying
    this direct-SQL pattern.
    """

    connection = sqlite3.connect(database_path)
    try:
        schema_version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        stream_id = connection.execute(
            """
            SELECT s.id
            FROM streams s
            JOIN instruments i ON i.id = s.instrument_id
            WHERE i.ticker = ? AND s.timeframe = ?
            """,
            (stream.instrument.ticker, stream.timeframe),
        ).fetchone()[0]
        candles = _count_rows(connection, "candles", stream_id)
        quarantine_rows = _count_rows(connection, "quarantine", stream_id)
        state = connection.execute(
            """
            SELECT COUNT(*), MAX(latest_committed_open_time_ms)
            FROM stream_state
            WHERE stream_id = ?
            """,
            (stream_id,),
        ).fetchone()
        open_times_ms = tuple(
            int(row[0])
            for row in connection.execute(
                """
                SELECT open_time_ms
                FROM candles
                WHERE stream_id = ?
                ORDER BY open_time_ms
                """,
                (stream_id,),
            )
        )
        return PersistenceSnapshot(
            schema_version=str(schema_version),
            candles=candles,
            stream_state_rows=int(state[0]),
            latest_committed_open_time_ms=state[1],
            quarantine_rows=quarantine_rows,
            open_times_ms=open_times_ms,
        )
    finally:
        connection.close()


def _check_wrong_symbol(stream: StreamKey, window: TimeWindow, clock: Clock) -> bool:
    source = BybitRestCandleSource(
        exchange_symbols={stream.instrument.ticker: "NOT_A_REAL_BYBIT_SYMBOL"},
        transport=StaticPayloadTransport({"retCode": 10001, "retMsg": "symbol invalid"}),
    )
    try:
        source.fetch_closed_candles(stream, window, observed_at_ms=clock.now_ms())
    except BybitApiError as exc:
        print(f"error_wrong_symbol=typed:{type(exc).__name__}")
        return True
    return False


def _check_empty_payload(stream: StreamKey, window: TimeWindow, clock: Clock) -> bool:
    source = BybitRestCandleSource(
        exchange_symbols={stream.instrument.ticker: "BTCUSDT"},
        transport=StaticPayloadTransport({"retCode": 0, "retMsg": "OK", "result": {"list": {}}}),
    )
    try:
        source.fetch_closed_candles(stream, window, observed_at_ms=clock.now_ms())
    except BybitPayloadError as exc:
        print(f"error_empty_payload=typed:{type(exc).__name__}")
        return True
    return False


def _check_invalid_candle(
    database_path: Path,
    stream: StreamKey,
    window: TimeWindow,
    clock: Clock,
) -> bool:
    step_ms = get_timeframe(stream.timeframe).duration_ms
    invalid = ObservedCandle(
        stream=stream,
        open_time_ms=window.start_ms,
        close_time_ms=window.start_ms + step_ms - 1,
        open="100",
        high="99",
        low="98",
        close="101",
        volume="1",
        confirmed=True,
        observed_at_ms=clock.now_ms(),
        source=ObservationSource.BYBIT_REST,
    )
    before = inspect_persistence(database_path, stream).quarantine_rows
    result = IngestObservedCandle(lambda: SqliteUnitOfWork(database_path)).execute(
        invalid,
        committed_at_ms=clock.now_ms(),
    )
    after = inspect_persistence(database_path, stream).quarantine_rows
    print(
        "error_invalid_candle="
        f"classification:{result.classification.value} "
        f"issues:{','.join(result.issue_codes)} "
        f"quarantined={after - before}"
    )
    return result.issue_codes == ("invalid_ohlc",) and after == before + 1


def _count_rows(connection: sqlite3.Connection, table: str, stream_id: int) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()[0]
    )


def is_contiguous_1m(open_times_ms: tuple[int, ...]) -> bool:
    return all(
        previous + 60_000 == current
        for previous, current in pairwise(open_times_ms)
    )
