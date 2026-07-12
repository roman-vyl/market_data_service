from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.full_bootstrap import (
    BootstrapFullStreamHistory,
    FullHistoryBootstrapRequest,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.domain import (
    InstrumentKey,
    InstrumentMetadata,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    StreamLifecycleState,
    TimeWindow,
)


@dataclass
class FakeClock:
    value: int = 300_000

    def now_ms(self) -> int:
        current = self.value
        self.value += 1
        return current


class FakeMetadataSource:
    def __init__(self, launch_time_ms: int) -> None:
        self.launch_time_ms = launch_time_ms
        self.calls: list[InstrumentKey] = []

    def get_launch_time_ms(self, instrument: InstrumentKey) -> int:
        self.calls.append(instrument)
        return self.launch_time_ms


class FakeHistoricalSource:
    def __init__(
        self,
        rows_by_start: dict[int, tuple[int, ...]] | None = None,
        *,
        fail_start_ms: int | None = None,
    ) -> None:
        self.rows_by_start = rows_by_start
        self.fail_start_ms = fail_start_ms
        self.calls: list[TimeWindow] = []

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        self.calls.append(window)
        if window.start_ms == self.fail_start_ms:
            raise RuntimeError("planned source failure")
        if self.rows_by_start is None:
            open_times = tuple(range(window.start_ms, window.end_ms, 60_000))
        else:
            open_times = self.rows_by_start.get(window.start_ms, ())
        return tuple(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open="100",
                high="101",
                low="99",
                close="100",
                volume="1",
                confirmed=True,
                observed_at_ms=observed_at_ms,
                source=ObservationSource.BYBIT_REST,
            )
            for open_time_ms in open_times
        )


def _stream(ticker: str = "BTCUSDT.P") -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def _prepare(path: Path, *streams: StreamKey) -> None:
    initialize_database(path)
    for stream in streams:
        register_stream(
            path,
            stream,
            exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
            now_ms=1,
        )


def _save_metadata(path: Path, stream: StreamKey, launch_time_ms: int) -> None:
    with SqliteUnitOfWork(path) as unit_of_work:
        unit_of_work.save_instrument_metadata(
            InstrumentMetadata(
                instrument=stream.instrument,
                exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
                launch_time_ms=launch_time_ms,
                fetched_at_ms=10,
            )
        )
        unit_of_work.commit()


def _save_lower_bound(path: Path, stream: StreamKey, open_time_ms: int) -> None:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            snapshot.__class__(
                stream=snapshot.stream,
                state=snapshot.state,
                earliest_available_open_time_ms=open_time_ms,
                latest_committed_open_time_ms=snapshot.latest_committed_open_time_ms,
                last_audit_at_ms=snapshot.last_audit_at_ms,
                last_rest_success_at_ms=snapshot.last_rest_success_at_ms,
                last_ws_message_at_ms=snapshot.last_ws_message_at_ms,
                last_error_code=snapshot.last_error_code,
                last_error_detail=snapshot.last_error_detail,
                state_changed_at_ms=snapshot.state_changed_at_ms,
                updated_at_ms=11,
            )
        )
        unit_of_work.commit()


def _state(path: Path, stream: StreamKey) -> tuple[str, int | None, int | None]:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
    return (
        snapshot.state.value,
        snapshot.earliest_available_open_time_ms,
        snapshot.latest_committed_open_time_ms,
    )


def _count_candles(path: Path, stream: StreamKey) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM candles c
                JOIN streams s ON s.id = c.stream_id
                JOIN instruments i ON i.id = s.instrument_id
                WHERE i.ticker = ? AND s.timeframe = ?
                """,
                (stream.instrument.ticker, stream.timeframe),
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _resolver(
    path: Path,
    metadata_source: FakeMetadataSource,
    historical_source: FakeHistoricalSource,
    clock: FakeClock,
) -> ResolveHistoricalLowerBound:
    return ResolveHistoricalLowerBound(
        metadata_source,
        historical_source,
        lambda: SqliteUnitOfWork(path),
        clock,
        max_candles_per_probe=2,
    )


def _workflow(
    path: Path,
    metadata_source: FakeMetadataSource,
    historical_source: FakeHistoricalSource,
    clock: FakeClock,
) -> BootstrapFullStreamHistory:
    importer = ImportHistoricalWindow(historical_source, lambda: SqliteUnitOfWork(path), clock)
    backfill = BackfillStreamHistory(
        importer,
        lambda: SqliteUnitOfWork(path),
        clock,
        max_candles_per_window=2,
    )
    return BootstrapFullStreamHistory(
        _resolver(path, metadata_source, historical_source, clock),
        backfill,
        lambda: SqliteUnitOfWork(path),
        clock,
    )


def test_saved_lower_bound_is_used_without_repeating_source_calls(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _save_metadata(path, stream, launch_time_ms=12_345)
    _save_lower_bound(path, stream, open_time_ms=120_000)
    metadata = FakeMetadataSource(launch_time_ms=0)
    source = FakeHistoricalSource(rows_by_start={})

    result = _resolver(path, metadata, source, FakeClock()).execute(stream)

    assert result.lower_bound_cached is True
    assert result.earliest_available_open_time_ms == 120_000
    assert metadata.calls == []
    assert source.calls == []


def test_launch_time_is_not_accepted_as_the_first_candle(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource(rows_by_start={60_000: (), 180_000: (180_000, 240_000)})

    result = _resolver(path, FakeMetadataSource(launch_time_ms=1), source, FakeClock()).execute(
        stream
    )

    assert result.search_start_time_ms == 60_000
    assert result.earliest_available_open_time_ms == 180_000
    assert _state(path, stream)[1] == 180_000


def test_unresolved_lower_bound_does_not_start_full_bootstrap(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource(rows_by_start={})

    result = _workflow(path, FakeMetadataSource(0), source, FakeClock()).execute(
        FullHistoryBootstrapRequest(stream, max_windows=1)
    )

    assert result.status == "lower_bound_unresolved"
    assert result.backfill is None
    assert _count_candles(path, stream) == 0
    assert _state(path, stream) == (StreamLifecycleState.BOOTSTRAPPING.value, None, None)


def test_full_bootstrap_obeys_budget_and_resumes_from_durable_progress(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    clock = FakeClock()
    source = FakeHistoricalSource()
    workflow = _workflow(path, FakeMetadataSource(0), source, clock)

    first = workflow.execute(FullHistoryBootstrapRequest(stream, max_windows=1))
    source.calls.clear()
    second = workflow.execute(FullHistoryBootstrapRequest(stream, max_windows=1))

    assert first.backfill is not None
    assert first.backfill.completed_windows == 1
    assert first.reached_target is False
    assert _state(path, stream)[0] == StreamLifecycleState.BOOTSTRAPPING.value
    assert second.lower_bound is not None
    assert second.lower_bound.lower_bound_cached is True
    assert second.backfill is not None
    assert second.backfill.window_results[0].window == TimeWindow(120_000, 240_000)
    assert _count_candles(path, stream) == 4


def test_full_bootstrap_reaching_target_moves_to_auditing(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    result = _workflow(path, FakeMetadataSource(0), FakeHistoricalSource(), FakeClock()).execute(
        FullHistoryBootstrapRequest(stream, max_windows=10)
    )

    assert result.reached_target is True
    assert _state(path, stream)[0] == StreamLifecycleState.AUDITING.value


def test_full_bootstrap_state_is_scoped_per_stream(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _prepare(path, btc, eth)

    _workflow(path, FakeMetadataSource(0), FakeHistoricalSource(), FakeClock()).execute(
        FullHistoryBootstrapRequest(btc, max_windows=1)
    )

    assert _state(path, btc)[1:] == (0, 60_000)
    assert _state(path, eth) == (StreamLifecycleState.UNINITIALIZED.value, None, None)


def test_lower_bound_source_failure_does_not_create_false_progress(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    with pytest.raises(RuntimeError, match="planned source failure"):
        _resolver(
            path,
            FakeMetadataSource(0),
            FakeHistoricalSource(fail_start_ms=0),
            FakeClock(),
        ).execute(stream)

    assert _state(path, stream)[1:] == (None, None)


def test_full_bootstrap_lower_bound_source_failure_records_failure_without_progress(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    result = _workflow(
        path,
        FakeMetadataSource(0),
        FakeHistoricalSource(fail_start_ms=0),
        FakeClock(),
    ).execute(FullHistoryBootstrapRequest(stream, max_windows=1))

    assert result.error_code == "RuntimeError"
    assert result.backfill is None
    assert _count_candles(path, stream) == 0
    assert _state(path, stream) == (StreamLifecycleState.FAILED.value, None, None)
