"""Real end-to-end bounded backfill smoke runner."""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.smoke_backfill import (
    SmokeBackfillWorkflowResult,
    run_backfill_smoke_workflow,
)
from market_data_service.domain import (
    InstrumentKey,
    StreamKey,
    TimeWindow,
    get_timeframe,
    last_closed_open_time_ms,
)
from market_data_service.entrypoints.smoke_support import (
    PersistenceSnapshot,
    inspect_persistence,
    is_contiguous_1m,
)


@dataclass(frozen=True, slots=True)
class SmokeBackfillResult:
    database_path: Path
    workflow: SmokeBackfillWorkflowResult
    persistence: PersistenceSnapshot
    continuity_ok: bool

    @property
    def ok(self) -> bool:
        return (
            self.workflow.first_committed > 0
            and self.workflow.first_duplicates == 0
            and self.workflow.duplicate_committed == 0
            and self.workflow.duplicate_duplicates > 0
            and self.persistence.schema_version == "1"
            and self.persistence.candles == self.workflow.first_committed
            and self.persistence.stream_state_rows == 1
            and self.persistence.latest_committed_open_time_ms is not None
            and self.persistence.quarantine_rows == 0
            and self.continuity_ok
        )


class Clock:
    def now_ms(self) -> int:
        import time

        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.database is not None:
        args.database.parent.mkdir(parents=True, exist_ok=True)
        return _run(args.database, args)

    with tempfile.TemporaryDirectory(prefix="market-data-smoke-backfill-") as directory:
        database = Path(directory) / "smoke.sqlite3"
        status = _run(database, args)
        if args.keep_database:
            keep_path = Path.cwd() / "tmp" / database.name
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.write_bytes(database.read_bytes())
            print(f"kept_database={keep_path}")
        return status


def _run(database: Path, args: argparse.Namespace) -> int:
    result = run_smoke_backfill(
        database_path=database,
        ticker=args.ticker,
        exchange_symbol=args.bybit_symbol,
        timeframe_id=args.timeframe,
        minutes=args.minutes,
    )
    _print_result(result)
    return 0 if result.ok else 1


def run_smoke_backfill(
    *,
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    timeframe_id: str,
    minutes: int,
) -> SmokeBackfillResult:
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

    workflow = run_backfill_smoke_workflow(
        stream=stream,
        window=window,
        backfill=backfill,
        duplicate_replay=importer,
    )
    persistence = inspect_persistence(database_path, stream)
    return SmokeBackfillResult(
        database_path=database_path,
        workflow=workflow,
        persistence=persistence,
        continuity_ok=is_contiguous_1m(persistence.open_times_ms),
    )


def _recent_closed_window(clock: Clock, *, timeframe_id: str, minutes: int) -> TimeWindow:
    if minutes <= 0 or minutes > 1000:
        raise ValueError("minutes must be between 1 and 1000")
    timeframe = get_timeframe(timeframe_id)
    latest_closed_open_ms = last_closed_open_time_ms(clock.now_ms(), timeframe.duration_ms)
    end_ms = latest_closed_open_ms + timeframe.duration_ms
    return TimeWindow(end_ms - minutes * timeframe.duration_ms, end_ms)


def _print_result(result: SmokeBackfillResult) -> None:
    workflow = result.workflow
    persistence = result.persistence
    print(f"database={result.database_path}")
    print(f"stream={workflow.stream.canonical_id}")
    print(f"window=[{workflow.window.start_ms}, {workflow.window.end_ms})")
    print(
        "first_backfill "
        f"observed={workflow.first_observed} committed={workflow.first_committed} "
        f"duplicate={workflow.first_duplicates} corrected={workflow.first_corrected} "
        f"rejected={workflow.first_rejected}"
    )
    print(
        "duplicate_replay "
        f"observed={workflow.duplicate_observed} committed={workflow.duplicate_committed} "
        f"duplicate={workflow.duplicate_duplicates} corrected={workflow.duplicate_corrected} "
        f"rejected={workflow.duplicate_rejected}"
    )
    print(
        "persistence "
        f"schema_version={persistence.schema_version} candles={persistence.candles} "
        f"stream_state_rows={persistence.stream_state_rows} "
        f"latest_committed_open_time_ms={persistence.latest_committed_open_time_ms} "
        f"quarantine_rows={persistence.quarantine_rows}"
    )
    print(f"continuity_1m={'PASS' if result.continuity_ok else 'FAIL'}")
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
