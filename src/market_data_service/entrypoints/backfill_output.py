"""Formatting for administrative backfill commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from market_data_service.application.full_bootstrap import FullHistoryBootstrapResult
from market_data_service.application.multi_stream_backfill import MultiStreamBackfillResult


def print_range_result(
    database: Path,
    exchange_symbol: str,
    result: Any,
    start_ms: int,
    end_ms: int,
    max_windows: int,
) -> None:
    print(f"database={database}")
    print(f"stream={result.stream.canonical_id} bybit_symbol={exchange_symbol}")
    print(f"requested_window=[{start_ms}, {end_ms}) max_windows={max_windows}")
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


def print_all_result(
    database: Path,
    result: MultiStreamBackfillResult,
    max_windows: int,
) -> None:
    print(f"database={database}")
    print(f"all_streams=true max_windows_per_stream={max_windows}")
    for index, outcome in enumerate(result.outcomes, start=1):
        item = outcome.result
        if item is None:
            print(
                f"stream[{index}]={outcome.stream.canonical_id} "
                "status=metadata_failed discovery_windows=0 backfill_windows=0 "
                f"total_windows=0 target_reached=false "
                f"failure_disposition={outcome.failure_disposition} "
                f"error_code={outcome.error_code}"
            )
            continue
        print(
            f"stream[{index}]={item.stream.canonical_id} "
            f"status={item.status} discovery_windows={item.discovery_windows_used} "
            f"backfill_windows={item.backfill_windows_attempted} "
            f"total_windows={item.total_windows_used} "
            f"target_reached={str(item.reached_target).lower()} "
            f"failure_disposition={item.failure_disposition} error_code={item.error_code}"
        )
    print(f"all_streams_result={result.status}")


def print_full_result(
    database: Path,
    exchange_symbol: str,
    result: FullHistoryBootstrapResult,
    max_windows: int,
) -> None:
    print(f"database={database}")
    print(f"stream={result.stream.canonical_id} bybit_symbol={exchange_symbol}")
    print(f"full_history=true max_windows={max_windows}")
    print(
        "budget "
        f"max_windows={result.max_windows} "
        f"discovery_windows={result.discovery_windows_used} "
        f"backfill_windows={result.backfill_windows_attempted} "
        f"total_windows={result.total_windows_used} "
        f"lower_bound_resolved={str(result.lower_bound_resolved).lower()} "
        f"target_reached={str(result.reached_target).lower()}"
    )
    if result.lower_bound is not None:
        lower_bound = result.lower_bound
        print(
            "lower_bound "
            f"launch_time_ms={lower_bound.launch_time_ms} "
            f"search_start_time_ms={lower_bound.search_start_time_ms} "
            f"observed_earliest_open_time_ms={lower_bound.earliest_available_open_time_ms} "
            f"metadata_cached={str(lower_bound.metadata_cached).lower()} "
            f"lower_bound_cached={str(lower_bound.lower_bound_cached).lower()} "
            f"unresolved_reason={lower_bound.unresolved_reason}"
        )
    print(f"target_open_time_ms={result.target_open_time_ms}")
    if result.backfill is not None:
        for index, window_result in enumerate(result.backfill.window_results, start=1):
            window = window_result.window
            print(
                f"window[{index}]=[{window.start_ms}, {window.end_ms}) "
                f"observed={window_result.observed} committed={window_result.committed} "
                f"duplicate={window_result.duplicates} corrected={window_result.corrected} "
                f"rejected={window_result.rejected} unexpected={window_result.unexpected}"
            )
        print(
            f"backfill_result completed_windows={result.backfill.completed_windows} "
            f"attempted_windows={result.backfill.attempted_windows} "
            f"reached_target={str(result.reached_target).lower()} "
            f"next_start_time_ms={result.backfill.next_start_time_ms}"
        )
    print(f"full_bootstrap_status={result.status}")
    if result.error_code is not None:
        print(
            f"full_bootstrap_error code={result.error_code} detail={result.error_detail} "
            f"disposition={result.failure_disposition}"
        )
