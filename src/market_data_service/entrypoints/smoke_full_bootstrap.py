"""Real Bybit full-history bootstrap restart/resume smoke runner."""

from __future__ import annotations

import argparse
import tempfile
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
from market_data_service.application.full_bootstrap import (
    BootstrapFullStreamHistory,
    FullHistoryBootstrapRequest,
    FullHistoryBootstrapResult,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.domain import InstrumentKey, StreamKey


@dataclass(frozen=True, slots=True)
class SmokeFullBootstrapResult:
    database_path: Path
    first: FullHistoryBootstrapResult
    second: FullHistoryBootstrapResult

    @property
    def ok(self) -> bool:
        if self.first.total_windows_used > self.first.max_windows:
            return False
        if self.second.total_windows_used > self.second.max_windows:
            return False
        if self.first.lower_bound is None or not self.first.lower_bound.resolved:
            return False
        if self.second.lower_bound is None or self.second.backfill is None:
            return False
        if not self.second.lower_bound.lower_bound_cached:
            return False
        if not self.second.backfill.window_results:
            return False
        if self.first.backfill is None:
            return (
                self.second.backfill.window_results[0].window.start_ms
                == self.first.lower_bound.earliest_available_open_time_ms
            )
        if not self.first.backfill.window_results:
            return False
        return (
            self.second.backfill.window_results[0].window.start_ms
            == self.first.backfill.next_start_time_ms
        )


class Clock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.database is not None:
        args.database.parent.mkdir(parents=True, exist_ok=True)
        return _run(args.database, args)

    with tempfile.TemporaryDirectory(prefix="market-data-smoke-full-bootstrap-") as directory:
        return _run(Path(directory) / "smoke.sqlite3", args)


def _run(database: Path, args: argparse.Namespace) -> int:
    result = run_smoke_full_bootstrap(
        database_path=database,
        ticker=args.ticker,
        exchange_symbol=args.bybit_symbol,
        max_windows=args.max_windows,
    )
    _print_result(result)
    return 0 if result.ok else 1


def run_smoke_full_bootstrap(
    *,
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    max_windows: int,
) -> SmokeFullBootstrapResult:
    clock = Clock()
    stream = StreamKey(InstrumentKey(ticker), "1m")
    initialize_database(database_path)
    register_stream(database_path, stream, exchange_symbol=exchange_symbol, now_ms=clock.now_ms())

    first = _new_workflow(database_path, ticker, exchange_symbol, clock).execute(
        FullHistoryBootstrapRequest(stream=stream, max_windows=max_windows)
    )
    second = _new_workflow(database_path, ticker, exchange_symbol, clock).execute(
        FullHistoryBootstrapRequest(stream=stream, max_windows=max_windows)
    )
    return SmokeFullBootstrapResult(database_path=database_path, first=first, second=second)


def _new_workflow(
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    clock: Clock,
) -> BootstrapFullStreamHistory:
    source = BybitRestCandleSource(exchange_symbols={ticker: exchange_symbol})

    def unit_of_work_factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database_path)

    importer = ImportHistoricalWindow(source, unit_of_work_factory, clock)
    backfill = BackfillStreamHistory(
        importer,
        unit_of_work_factory,
        clock,
        max_candles_per_window=1000,
    )
    lower_bound = ResolveHistoricalLowerBound(
        source,
        source,
        unit_of_work_factory,
        clock,
        max_candles_per_probe=1000,
    )
    return BootstrapFullStreamHistory(lower_bound, backfill, unit_of_work_factory, clock)


def _print_result(result: SmokeFullBootstrapResult) -> None:
    print(f"database={result.database_path}")
    _print_invocation("first", result.first)
    _print_invocation("second", result.second)
    print(f"smoke_result={'PASS' if result.ok else 'FAIL'}")


def _print_invocation(label: str, result: FullHistoryBootstrapResult) -> None:
    print(f"{label}_status={result.status}")
    print(
        f"{label}_budget "
        f"discovery_windows={result.discovery_windows_used} "
        f"backfill_windows={result.backfill_windows_attempted} "
        f"total_windows={result.total_windows_used} "
        f"max_windows={result.max_windows} "
        f"lower_bound_resolved={str(result.lower_bound_resolved).lower()} "
        f"target_reached={str(result.reached_target).lower()}"
    )
    if result.lower_bound is not None:
        print(
            f"{label}_lower_bound "
            f"launch_time_ms={result.lower_bound.launch_time_ms} "
            f"observed_earliest_open_time_ms="
            f"{result.lower_bound.earliest_available_open_time_ms} "
            f"cached={str(result.lower_bound.lower_bound_cached).lower()} "
            f"unresolved_reason={result.lower_bound.unresolved_reason}"
        )
    if result.backfill is not None:
        first_window = (
            result.backfill.window_results[0].window if result.backfill.window_results else None
        )
        print(
            f"{label}_backfill "
            f"completed_windows={result.backfill.completed_windows} "
            f"reached_target={str(result.reached_target).lower()} "
            f"first_window={first_window} "
            f"next_start_time_ms={result.backfill.next_start_time_ms}"
        )
    if result.error_code is not None:
        print(f"{label}_error code={result.error_code} detail={result.error_detail}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--ticker", default="BTCUSDT.P")
    parser.add_argument("--bybit-symbol", default="BTCUSDT")
    parser.add_argument("--max-windows", type=int, default=20)
    return parser
