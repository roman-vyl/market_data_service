"""Real Bybit two-stream bounded backfill and durable-resume smoke."""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import tempfile
from pathlib import Path

from market_data_service.entrypoints.backfill import main as backfill_main


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    database, cleanup = _database_path(args.keep_database)
    try:
        common = [
            "--database",
            str(database),
            "--config",
            str(args.config),
            "--all",
            "--full",
            "--max-windows",
            str(args.max_windows),
        ]
        first_code = backfill_main(common)
        first = _progress(database)
        second_code = backfill_main(common)
        second = _progress(database)
        streams = sorted(set(first) | set(second))
        passed = (
            first_code == 0
            and second_code == 0
            and bool(streams)
            and all(stream in first and stream in second for stream in streams)
            and all(second[stream] > first[stream] for stream in streams)
        )
        print(f"smoke_database={database}")
        for stream in streams:
            print(
                f"resume stream={stream} first_latest={first.get(stream)} "
                f"second_latest={second.get(stream)}"
            )
        print(f"smoke_result={'PASS' if passed else 'FAIL'}")
        return 0 if passed else 1
    finally:
        if cleanup:
            shutil.rmtree(database.parent, ignore_errors=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/markets.toml"))
    parser.add_argument("--max-windows", type=int, default=20)
    parser.add_argument("--keep-database", action="store_true")
    return parser


def _database_path(keep: bool) -> tuple[Path, bool]:
    root = Path(tempfile.mkdtemp(prefix="market-data-all-backfill-"))
    return root / "market.sqlite3", not keep


def _progress(database: Path) -> dict[str, int]:
    connection = sqlite3.connect(database)
    try:
        rows = connection.execute(
            """
            SELECT i.ticker, s.timeframe, ss.latest_committed_open_time_ms
            FROM stream_state ss
            JOIN streams s ON s.id = ss.stream_id
            JOIN instruments i ON i.id = s.instrument_id
            ORDER BY i.id, s.id
            """
        ).fetchall()
    finally:
        connection.close()
    return {
        f"{ticker}:{timeframe}": int(latest)
        for ticker, timeframe, latest in rows
        if latest is not None
    }
