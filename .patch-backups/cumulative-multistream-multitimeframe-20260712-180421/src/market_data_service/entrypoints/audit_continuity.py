"""Administrative continuity audit command."""

from __future__ import annotations

import argparse
from pathlib import Path

from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.domain import ContinuityReport, InstrumentKey, StreamKey
from market_data_service.entrypoints.market_config import (
    entry_for_ticker,
    load_enabled_market_entries,
)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_enabled_market_entries(args.config)
    entry = entry_for_ticker(config, args.ticker)
    stream = StreamKey(InstrumentKey(entry.ticker), args.timeframe)
    auditor = AuditStreamContinuity(lambda: SqliteUnitOfWork(args.database))
    report = auditor.execute(
        AuditStreamContinuityRequest(
            stream=stream,
            start_time_ms=args.start,
            end_time_ms=args.end,
        )
    )
    _print_report(args.database, report)
    return 0 if report.is_continuous else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/market.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/markets.toml"))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    return parser


def _print_report(database: Path, report: ContinuityReport) -> None:
    print(f"database={database}")
    print(f"stream={report.stream.canonical_id}")
    print(f"checked_window=[{report.checked_start_ms}, {report.checked_end_ms})")
    print(
        "continuity_result "
        f"is_continuous={str(report.is_continuous).lower()} "
        f"candle_count={report.candle_count} gap_count={len(report.gaps)}"
    )
    for index, gap in enumerate(report.gaps, start=1):
        print(f"gap[{index}]=[{gap.start_ms}, {gap.end_ms})")
