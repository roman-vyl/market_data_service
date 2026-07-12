"""Real REST backfill plus continuity audit smoke runner."""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.smoke_audit_continuity import (
    SmokeAuditContinuityResult,
    run_smoke_audit_continuity_workflow,
)
from market_data_service.domain import (
    InstrumentKey,
    StreamKey,
    TimeWindow,
    get_timeframe,
    last_closed_open_time_ms,
)


class Clock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.database is not None:
        args.database.parent.mkdir(parents=True, exist_ok=True)
        return _run(args.database, args)

    with tempfile.TemporaryDirectory(prefix="market-data-smoke-audit-") as directory:
        database = Path(directory) / "smoke.sqlite3"
        status = _run(database, args)
        if args.keep_database:
            keep_path = Path.cwd() / "tmp" / database.name
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.write_bytes(database.read_bytes())
            print(f"kept_database={keep_path}")
        return status


def _run(database: Path, args: argparse.Namespace) -> int:
    result = run_smoke_audit_continuity(
        database_path=database,
        ticker=args.ticker,
        exchange_symbol=args.bybit_symbol,
        timeframe_id=args.timeframe,
        minutes=args.minutes,
    )
    _print_result(result)
    return 0 if result.ok else 1


def run_smoke_audit_continuity(
    *,
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    timeframe_id: str,
    minutes: int,
) -> SmokeAuditContinuityResult:
    clock = Clock()
    stream = StreamKey(InstrumentKey(ticker), timeframe_id)
    window = _recent_closed_window(clock, timeframe_id=timeframe_id, minutes=minutes)

    initialize_database(database_path)
    register_stream(database_path, stream, exchange_symbol=exchange_symbol, now_ms=clock.now_ms())

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
    auditor = AuditStreamContinuity(unit_of_work_factory)
    return run_smoke_audit_continuity_workflow(
        stream=stream,
        window=window,
        backfill=backfill,
        auditor=auditor,
    )


def _recent_closed_window(clock: Clock, *, timeframe_id: str, minutes: int) -> TimeWindow:
    if minutes <= 0 or minutes > 1000:
        raise ValueError("minutes must be between 1 and 1000")
    timeframe = get_timeframe(timeframe_id)
    latest_closed_open_ms = last_closed_open_time_ms(clock.now_ms(), timeframe.duration_ms)
    end_ms = latest_closed_open_ms + timeframe.duration_ms
    return TimeWindow(end_ms - minutes * timeframe.duration_ms, end_ms)


def _print_result(result: SmokeAuditContinuityResult) -> None:
    audit = result.audit
    print(f"stream={result.stream.canonical_id}")
    print(f"window=[{result.window.start_ms}, {result.window.end_ms})")
    print(
        "backfill "
        f"observed={result.backfill_observed} committed={result.backfill_committed} "
        f"duplicate={result.backfill_duplicates} corrected={result.backfill_corrected} "
        f"rejected={result.backfill_rejected}"
    )
    print(
        "audit "
        f"candles={audit.candle_count} continuity={str(audit.is_continuous).lower()} "
        f"gaps={len(audit.gaps)}"
    )
    print(f"smoke_result={'PASS' if result.ok else 'FAIL'}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--keep-database", action="store_true")
    parser.add_argument("--ticker", default="BTCUSDT.P")
    parser.add_argument("--bybit-symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--minutes", type=int, default=120)
    return parser
