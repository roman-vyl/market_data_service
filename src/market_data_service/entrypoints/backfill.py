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
from market_data_service.application.full_bootstrap import (
    BootstrapFullStreamHistory,
    FullHistoryBootstrapRequest,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.market_metadata import VerifyConfiguredInstrumentMetadata
from market_data_service.application.multi_stream_backfill import (
    BackfillAllConfiguredStreams,
    MultiStreamBackfillRequest,
)
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain import InstrumentCoverage, StreamKey, get_timeframe
from market_data_service.entrypoints.backfill_output import (
    print_all_result,
    print_full_result,
    print_range_result,
)
from market_data_service.entrypoints.market_config import (
    entry_for_ticker,
    load_validated_market_config,
)


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_validated_market_config(args.config)
    if args.all:
        return _run_all(args, config)
    if args.ticker is None:
        raise ValueError("provide --ticker or --all")
    return _run_one(args, config)


def _run_one(args: argparse.Namespace, config: ValidatedMarketConfig) -> int:
    entry = entry_for_ticker(config.enabled_instruments, args.ticker)
    stream = next(
        candidate
        for candidate in entry.stream_keys
        if candidate.timeframe == args.timeframe
    )
    clock = SystemClock()
    source = _source(config)
    VerifyConfiguredInstrumentMetadata(source, category=config.source.category).execute(entry)
    _prepare_stream(args.database, stream, entry, clock)
    backfill = _build_backfill(args.database, source, clock)

    if args.full:
        if args.start is not None or args.end is not None or args.minutes is not None:
            raise ValueError("--full cannot be combined with --start/--end/--minutes")
        full_result = _build_full_bootstrap(args.database, source, backfill, clock).execute(
            FullHistoryBootstrapRequest(stream=stream, max_windows=args.max_windows)
        )
        print_full_result(args.database, entry.exchange_symbol, full_result, args.max_windows)
        return 0 if full_result.error_code is None else 1

    start_ms, end_ms = _resolve_range(args, stream)
    result = backfill.execute(
        BackfillStreamRequest(
            stream=stream,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            max_windows=args.max_windows,
        )
    )
    print_range_result(
        args.database,
        entry.exchange_symbol,
        result,
        start_ms,
        end_ms,
        args.max_windows,
    )
    return 0 if result.error_code is None else 1


def _run_all(args: argparse.Namespace, config: ValidatedMarketConfig) -> int:
    if not args.full:
        raise ValueError("--all requires --full")
    if args.start is not None or args.end is not None or args.minutes is not None:
        raise ValueError("--all --full cannot be combined with --start/--end/--minutes")
    clock = SystemClock()
    source = _source(config)
    verifier = VerifyConfiguredInstrumentMetadata(source, category=config.source.category)

    def bootstrap_factory(
        coverage: InstrumentCoverage, stream: StreamKey
    ) -> BootstrapFullStreamHistory:
        _prepare_stream(args.database, stream, coverage, clock)
        backfill = _build_backfill(args.database, source, clock)
        return _build_full_bootstrap(args.database, source, backfill, clock)

    result = BackfillAllConfiguredStreams(verifier.execute, bootstrap_factory).execute(
        config.enabled_instruments,
        MultiStreamBackfillRequest(max_windows_per_stream=args.max_windows),
    )
    print_all_result(args.database, result, args.max_windows)
    return 1 if result.has_errors else 0


def _source(config: ValidatedMarketConfig) -> BybitRestCandleSource:
    return BybitRestCandleSource(
        exchange_symbols=config.exchange_symbols,
        category=config.source.category,
    )


def _prepare_stream(
    database: Path,
    stream: StreamKey,
    coverage: InstrumentCoverage,
    clock: SystemClock,
) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(database)
    register_stream(
        database,
        stream,
        exchange_symbol=coverage.exchange_symbol,
        now_ms=clock.now_ms(),
    )


def _build_backfill(
    database: Path,
    source: BybitRestCandleSource,
    clock: SystemClock,
) -> BackfillStreamHistory:
    importer = ImportHistoricalWindow(source, lambda: SqliteUnitOfWork(database), clock)
    return BackfillStreamHistory(importer, lambda: SqliteUnitOfWork(database), clock)


def _build_full_bootstrap(
    database: Path,
    source: BybitRestCandleSource,
    backfill: BackfillStreamHistory,
    clock: SystemClock,
) -> BootstrapFullStreamHistory:
    lower_bound = ResolveHistoricalLowerBound(
        source,
        source,
        lambda: SqliteUnitOfWork(database),
        clock,
    )
    return BootstrapFullStreamHistory(
        lower_bound,
        backfill,
        lambda: SqliteUnitOfWork(database),
        clock,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/market.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/markets.toml"))
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--ticker")
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    parser.add_argument("--minutes", type=int)
    parser.add_argument("--max-windows", type=int, default=1)
    parser.add_argument("--full", action="store_true")
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


