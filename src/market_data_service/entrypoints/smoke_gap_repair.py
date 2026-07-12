"""Real Bybit bounded backfill plus production gap-repair smoke runner."""

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
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
)
from market_data_service.domain import (
    ContinuityReport,
    InstrumentKey,
    StreamKey,
    TimeWindow,
    get_timeframe,
    last_closed_open_time_ms,
)
from market_data_service.entrypoints.smoke_gap_report import print_smoke_gap_repair_result
from market_data_service.entrypoints.smoke_gap_support import delete_candle_for_smoke


@dataclass(frozen=True, slots=True)
class SmokeGapRepairResult:
    database_path: Path
    stream: StreamKey
    window: TimeWindow
    deleted_open_time_ms: int
    initial_audit: ContinuityReport
    gap_audit: ContinuityReport
    repair: RepairStreamGapsResult
    repeated_repair: RepairStreamGapsResult

    @property
    def ok(self) -> bool:
        return (
            self.initial_audit.is_continuous
            and not self.gap_audit.is_continuous
            and len(self.gap_audit.gaps) == 1
            and self.repair.status is RepairStatus.COMPLETE
            and self.repair.post_repair_audit is not None
            and self.repair.post_repair_audit.is_continuous
            and self.repeated_repair.status is RepairStatus.COMPLETE
            and self.repeated_repair.attempted_windows == 0
        )


class Clock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.database is not None:
        args.database.parent.mkdir(parents=True, exist_ok=True)
        return _run(args.database, args)

    with tempfile.TemporaryDirectory(prefix="market-data-smoke-gap-repair-") as directory:
        database = Path(directory) / "smoke.sqlite3"
        status = _run(database, args)
        if args.keep_database:
            keep_path = Path.cwd() / "tmp" / database.name
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.write_bytes(database.read_bytes())
            print(f"kept_database={keep_path}")
        return status


def _run(database: Path, args: argparse.Namespace) -> int:
    result = run_smoke_gap_repair(
        database_path=database,
        ticker=args.ticker,
        exchange_symbol=args.bybit_symbol,
        timeframe_id=args.timeframe,
        minutes=args.minutes,
    )
    print_smoke_gap_repair_result(result)
    return 0 if result.ok else 1


def run_smoke_gap_repair(
    *,
    database_path: Path,
    ticker: str,
    exchange_symbol: str,
    timeframe_id: str,
    minutes: int,
) -> SmokeGapRepairResult:
    if minutes < 3:
        raise ValueError("minutes must be at least 3 so an internal candle can be deleted")
    clock = Clock()
    stream = StreamKey(InstrumentKey(ticker), timeframe_id)
    window = _recent_closed_window(clock, timeframe_id=timeframe_id, minutes=minutes)
    step_ms = get_timeframe(timeframe_id).duration_ms
    deleted_open_time_ms = window.start_ms + step_ms

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
    backfill_result = backfill.execute(
        BackfillStreamRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
            max_windows=1,
        )
    )
    if backfill_result.error_code is not None:
        raise RuntimeError(
            f"backfill failed: {backfill_result.error_code}: {backfill_result.error_detail}"
        )

    auditor = AuditStreamContinuity(unit_of_work_factory)
    initial_audit = auditor.execute(
        _audit_request(stream=stream, window=window)
    )
    delete_candle_for_smoke(database_path, stream, deleted_open_time_ms)
    gap_audit = auditor.execute(_audit_request(stream=stream, window=window))

    repair_use_case = RepairStreamGaps(
        auditor,
        importer,
        unit_of_work_factory,
        clock,
        max_candles_per_window=1000,
    )
    repair = repair_use_case.execute(
        RepairStreamGapsRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
            max_windows=1,
        )
    )
    repeated_repair = repair_use_case.execute(
        RepairStreamGapsRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
            max_windows=1,
        )
    )
    return SmokeGapRepairResult(
        database_path=database_path,
        stream=stream,
        window=window,
        deleted_open_time_ms=deleted_open_time_ms,
        initial_audit=initial_audit,
        gap_audit=gap_audit,
        repair=repair,
        repeated_repair=repeated_repair,
    )


def _audit_request(*, stream: StreamKey, window: TimeWindow) -> AuditStreamContinuityRequest:
    return AuditStreamContinuityRequest(
        stream=stream,
        start_time_ms=window.start_ms,
        end_time_ms=window.end_ms,
    )


def _recent_closed_window(clock: Clock, *, timeframe_id: str, minutes: int) -> TimeWindow:
    if minutes <= 0 or minutes > 1000:
        raise ValueError("minutes must be between 1 and 1000")
    timeframe = get_timeframe(timeframe_id)
    latest_closed_open_ms = last_closed_open_time_ms(clock.now_ms(), timeframe.duration_ms)
    end_ms = latest_closed_open_ms + timeframe.duration_ms
    return TimeWindow(end_ms - minutes * timeframe.duration_ms, end_ms)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--keep-database", action="store_true")
    parser.add_argument("--ticker", default="BTCUSDT.P")
    parser.add_argument("--bybit-symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--minutes", type=int, default=5)
    return parser
