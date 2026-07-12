"""Administrative bounded historical backfill command."""

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
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.domain import InstrumentKey, StreamKey, get_timeframe
from market_data_service.entrypoints.market_config import (
    entry_for_ticker,
    load_enabled_market_entries,
)


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_enabled_market_entries(args.config)
    entry = entry_for_ticker(config, args.ticker)
    stream = StreamKey(InstrumentKey(entry.ticker), args.timeframe)
    start_ms, end_ms = _resolve_range(args, stream)
    clock = SystemClock()

    args.database.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(args.database)
    register_stream(
        args.database,
        stream,
        exchange_symbol=entry.exchange_symbol,
        now_ms=clock.now_ms(),
    )

    source = BybitRestCandleSource(
        exchange_symbols={item.ticker: item.exchange_symbol for item in config}
    )
    importer = ImportHistoricalWindow(
        source,
        lambda: SqliteUnitOfWork(args.database),
        clock,
    )
    backfill = BackfillStreamHistory(
        importer,
        lambda: SqliteUnitOfWork(args.database),
        clock,
    )
    result = backfill.execute(
        BackfillStreamRequest(
            stream=stream,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            max_windows=args.max_windows,
        )
    )

    print(f"database={args.database}")
    print(f"stream={stream.canonical_id} bybit_symbol={entry.exchange_symbol}")
    print(f"requested_window=[{start_ms}, {end_ms}) max_windows={args.max_windows}")
    for index, window_result in enumerate(result.window_results, start=1):
        window = window_result.window
        print(
            f"window[{index}]=[{window.start_ms}, {window.end_ms}) "
            f"observed={window_result.observed} committed={window_result.committed} "
            f"duplicate={window_result.duplicates} corrected={window_result.corrected} "
            f"rejected={window_result.rejected} unexpected={window_result.unexpected}"
        )
    print(
        f"backfill_result completed_windows={result.completed_windows} "
        f"attempted_windows={result.attempted_windows} "
        f"reached_end={str(result.reached_end).lower()} "
        f"next_start_time_ms={result.next_start_time_ms}"
    )
    if result.error_code is not None:
        print(f"backfill_error code={result.error_code} detail={result.error_detail}")
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/market.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/markets.toml"))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    parser.add_argument("--minutes", type=int)
    parser.add_argument("--max-windows", type=int, default=1)
    return parser


def _resolve_range(args: argparse.Namespace, stream: StreamKey) -> tuple[int, int]:
    step_ms = get_timeframe(stream.timeframe).duration_ms
    if args.minutes is not None:
        if args.start is not None or args.end is not None:
            raise ValueError("--minutes cannot be combined with --start/--end")
        if args.minutes <= 0:
            raise ValueError("--minutes must be positive")
        end_ms = int(time.time() * 1000)
        end_ms -= end_ms % step_ms
        return end_ms - args.minutes * step_ms, end_ms
    if args.start is None or args.end is None:
        raise ValueError("provide either --minutes or both --start and --end")
    return args.start, args.end
