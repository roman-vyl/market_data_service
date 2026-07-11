"""Local REST smoke runner for one bounded Bybit window."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.import_window import ImportHistoricalWindow, ImportWindowResult
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain import (
    InstrumentKey,
    StreamKey,
    TimeWindow,
    get_timeframe,
    last_closed_open_time_ms,
)
from market_data_service.entrypoints.smoke_support import inspect_persistence, run_error_scenarios


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def run_smoke(
    *,
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    timeframe_id: str,
    minutes: int,
) -> int:
    clock = SystemClock()
    stream = StreamKey(InstrumentKey(ticker), timeframe_id)
    timeframe = get_timeframe(timeframe_id)
    latest_closed_open_ms = last_closed_open_time_ms(clock.now_ms(), timeframe.duration_ms)
    end_ms = latest_closed_open_ms + timeframe.duration_ms
    start_ms = end_ms - minutes * timeframe.duration_ms
    window = TimeWindow(start_ms, end_ms)

    initialize_database(database_path)
    register_stream(database_path, stream, exchange_symbol=exchange_symbol, now_ms=clock.now_ms())

    source = BybitRestCandleSource(exchange_symbols={ticker: exchange_symbol})
    importer = ImportHistoricalWindow(
        source,
        IngestObservedCandle(lambda: SqliteUnitOfWork(database_path)),
        clock,
    )

    print(f"database={database_path}")
    print(f"stream={stream.canonical_id} bybit_symbol={exchange_symbol}")
    print(f"window=[{window.start_ms}, {window.end_ms}) minutes={minutes}")

    first = importer.execute(stream, window)
    second = importer.execute(stream, window)
    _print_import_result("first_import", first)
    _print_import_result("second_import", second)

    persistence = inspect_persistence(database_path, stream)
    print(
        "persistence "
        f"schema_version={persistence.schema_version} "
        f"candles={persistence.candles} "
        f"stream_state_rows={persistence.stream_state_rows} "
        f"latest_committed_open_time_ms={persistence.latest_committed_open_time_ms} "
        f"quarantine_rows={persistence.quarantine_rows}"
    )

    errors_ok = run_error_scenarios(database_path, stream, window, clock)
    ok = (
        first.committed > 0
        and first.duplicates == 0
        and second.committed == 0
        and second.duplicates > 0
        and persistence.schema_version == "1"
        and persistence.candles > 0
        and persistence.stream_state_rows == 1
        and persistence.latest_committed_open_time_ms is not None
        and errors_ok
    )
    print(f"smoke_result={'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def _print_import_result(label: str, result: ImportWindowResult) -> None:
    print(
        f"{label} observed={result.observed} committed={result.committed} "
        f"duplicate={result.duplicates} corrected={result.corrected} rejected={result.rejected}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--ticker", default="BTCUSDT.P")
    parser.add_argument("--bybit-symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--minutes", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database = args.database or Path(f"tmp/bybit-rest-smoke-{int(time.time())}.sqlite3")
    database.parent.mkdir(parents=True, exist_ok=True)
    return run_smoke(
        database_path=database,
        ticker=args.ticker,
        exchange_symbol=args.bybit_symbol,
        timeframe_id=args.timeframe,
        minutes=args.minutes,
    )
