"""Regression contracts for selective lifecycle synchronization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from tests.test_runtime_realtime import FakeRecovery, IdleConnector, _register_connecting

from market_data_service.adapters.sqlite import SqliteUnitOfWork, initialize_database
from market_data_service.adapters.sqlite.stream_state_reader import SqliteStreamStateReader
from market_data_service.application.realtime.events import (
    CandleObserved,
    Connected,
    Disconnected,
    HeartbeatObserved,
    RealtimeEvent,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.application.realtime.supervisor_types import RealtimeStreamFacts
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState, StreamStateSnapshot
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.committed_bar_notification import bar_notification
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.open_interest_publication import OpenInterestPublication
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.status import RuntimeStatusStore


class CountingReader(SqliteStreamStateReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.reads: list[StreamKey] = []
        self.on_read: Callable[[], None] = lambda: None

    def get_stream_state(self, stream: StreamKey) -> StreamStateSnapshot:
        self.reads.append(stream)
        self.on_read()
        return super().get_stream_state(stream)


@dataclass
class Harness:
    runtime: RuntimeRealtimeCoordinator
    supervisor: RealtimeSupervisor
    status: RuntimeStatusStore
    reader: CountingReader
    lifecycle: RuntimeLifecycleRecorder
    streams: tuple[StreamKey, ...]
    now: list[int]

    async def sweep(self) -> None:
        # Stop after this turn starts; the entire turn must still visit all streams.
        stop = asyncio.Event()
        self.reader.on_read = stop.set
        try:
            await self.runtime._stale_worker(stop)
        finally:
            self.reader.on_read = lambda: None


def _harness(tmp_path: Path, *, count: int = 2, oi: bool = False) -> Harness:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    streams = tuple(StreamKey(InstrumentKey(f"TEST{i}USDT.P"), "5m") for i in range(count))
    for stream in streams:
        _register_connecting(path, stream)
    now = [614_999]  # One millisecond before bar 300000 becomes required.
    topics = {f"kline.5.TEST{i}USDT": stream for i, stream in enumerate(streams)}
    supervisor = RealtimeSupervisor(
        streams,
        topics,
        lambda: now[0],
        initial_latest_open_time_ms=dict.fromkeys(streams, 0),
        initial_open_interest_open_time_ms={streams[0]: 0} if oi else None,
    )
    supervisor.observe_event(SubscriptionConfirmed(tuple(topics), now[0]))
    for stream in streams:
        supervisor.record_recovery_result(stream, restored=True)
    reader = CountingReader(path)
    lifecycle = RuntimeLifecycleRecorder(
        lambda: SqliteUnitOfWork(path), lambda: now[0], state_reader=reader
    )
    status = RuntimeStatusStore(streams)
    runtime = RuntimeRealtimeCoordinator(
        streams=streams,
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=supervisor,
        recovery=FakeRecovery({}),  # type: ignore[arg-type]
        lifecycle=lifecycle,
        status=status,
        admission=RealtimeAdmissionGate(streams),
        operation_gate=asyncio.Lock(),
        now_ms=lambda: now[0],
        max_recovery_windows=1,
    )
    runtime._sync_lifecycle()
    reader.reads.clear()
    return Harness(runtime, supervisor, status, reader, lifecycle, streams, now)


def _candle(stream: StreamKey, now_ms: int, *, confirmed: bool = False) -> CandleObserved:
    return CandleObserved(
        stream,
        ObservedCandle(
            stream,
            300_000,
            599_999,
            "1",
            "2",
            "1",
            "1.5",
            "3",
            confirmed,
            now_ms,
            ObservationSource.BYBIT_WEBSOCKET,
        ),
    )


def _ready(h: Harness) -> list[bool]:
    return [item["ready"] for item in h.status.readiness_document()["streams"]]


def test_transport_activity_avoids_reads_but_confirmed_keeps_both_full_sweeps(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path, count=40)
        stream = h.streams[0]
        events: list[RealtimeEvent] = [Connected(h.now[0]), HeartbeatObserved(h.now[0])]
        events.extend(_candle(stream, h.now[0] + i) for i in range(19))
        for event in events:
            await h.runtime.on_event(event)
        assert h.reader.reads == []
        assert h.supervisor.facts(stream).last_transport_activity_ms == h.now[0] + 18
        assert h.supervisor.facts(stream).last_successful_open_time_ms == 0
        await h.sweep()
        assert h.reader.reads == list(h.streams)  # No stale signals, still a full sweep.
        h.reader.reads.clear()
        await h.runtime.on_event(_candle(stream, h.now[0], confirmed=True))
        assert h.reader.reads == list(h.streams)
        assert h.supervisor.facts(stream).last_successful_open_time_ms == 0
        await h.runtime.on_outcome(
            RealtimeIngestionOutcome(stream, 300_000, RealtimeIngestionClassification.COMMITTED)
        )
        assert h.reader.reads == list(h.streams) * 2
        assert h.supervisor.facts(stream).last_successful_open_time_ms == 300_000

    asyncio.run(scenario())


@pytest.mark.parametrize("missing", ["candle", "oi"])
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_time_driven_readiness_refresh_at_close_plus_grace(
    tmp_path: Path, missing: str, offset: int
) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path, oi=missing == "oi")
        if missing == "oi":
            h.supervisor.observe_outcome(
                RealtimeIngestionOutcome(
                    h.streams[0], 300_000, RealtimeIngestionClassification.COMMITTED
                )
            )
        assert _ready(h) == [True, True]
        h.now[0] = 615_000 + offset
        await h.runtime.on_event(_candle(h.streams[0], h.now[0]))
        await h.runtime.on_event(HeartbeatObserved(h.now[0]))
        assert h.reader.reads == []
        assert _ready(h) == [True, True]  # Stored projection waits for the next sweep.
        assert h.supervisor.facts(h.streams[0]).data_ready is (offset < 0)
        assert h.supervisor.facts(h.streams[0]).published_through(0) is (offset < 0)
        await h.sweep()
        assert _ready(h) == [offset < 0, offset < 0]
        for stream in h.streams:
            state = h.lifecycle.snapshot(stream).state
            assert state is (
                StreamLifecycleState.READY if offset < 0 else StreamLifecycleState.DEGRADED
            )
        if offset >= 0:
            assert all(h.supervisor.facts(s).recovery_pending for s in h.streams)
            h.reader.reads.clear()
            await h.sweep()  # Recovery already pending: no new signals, still all streams.
            assert h.reader.reads == list(h.streams)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "classification",
    [RealtimeIngestionClassification.REJECTED, RealtimeIngestionClassification.FAILED],
)
def test_rejected_and_failed_outcomes_still_sync_all_streams(
    tmp_path: Path, classification: RealtimeIngestionClassification
) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path)
        await h.runtime.on_outcome(RealtimeIngestionOutcome(h.streams[0], 300_000, classification))
        assert set(h.reader.reads) == set(h.streams)
        expected = (
            StreamLifecycleState.FAILED
            if classification is RealtimeIngestionClassification.FAILED
            else StreamLifecycleState.DEGRADED
        )
        assert h.lifecycle.snapshot(h.streams[0]).state is expected
        assert _ready(h) == [False, True]

    asyncio.run(scenario())


def test_disconnect_still_revokes_all_projections(tmp_path: Path) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path)
        await h.runtime.on_event(Disconnected(None, None, h.now[0]))
        assert set(h.reader.reads) == set(h.streams)
        assert _ready(h) == [False, False]

    asyncio.run(scenario())


def test_oi_change_updates_only_its_projection_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path, oi=True)
        stream = h.streams[0]
        h.supervisor.observe_outcome(
            RealtimeIngestionOutcome(stream, 300_000, RealtimeIngestionClassification.COMMITTED)
        )
        h.now[0] = 615_000
        # Reflect the candle outcome, but do not run stale detection/recovery.
        h.runtime._sync_lifecycle()
        assert _ready(h) == [False, False]
        h.reader.reads.clear()
        callbacks: list[StreamKey] = []

        def refresh(changed: StreamKey) -> None:
            h.runtime.on_open_interest_changed(changed)
            assert _ready(h) == [True, False]
            callbacks.append(changed)

        original = RealtimeStreamFacts.published_through

        def published_through(facts: RealtimeStreamFacts, open_time_ms: int) -> bool:
            assert callbacks == [stream]
            assert _ready(h) == [True, False]
            return original(facts, open_time_ms)

        monkeypatch.setattr(RealtimeStreamFacts, "published_through", published_through)
        publication = OpenInterestPublication(
            streams=(stream,),
            append=lambda s, t: True,
            supervisor=h.supervisor,
            request_recovery=h.runtime.request_recovery,
            on_open_interest_changed=refresh,
        )
        assert await publication.prepare(bar_notification(stream, 300_000))
        assert callbacks == [stream]
        assert h.reader.reads and set(h.reader.reads) == {stream}
        h.reader.reads.clear()
        assert await publication.prepare(bar_notification(stream, 300_000))
        assert callbacks == [stream]  # Unchanged duplicate.
        assert h.reader.reads == []

    asyncio.run(scenario())


@pytest.mark.parametrize("append_result", [False, "error", True])
def test_oi_failure_or_discontinuity_callback(tmp_path: Path, append_result: bool | str) -> None:
    async def scenario() -> None:
        h = _harness(tmp_path, oi=True)
        stream = h.streams[0]
        order: list[str] = []

        async def recovery(signal) -> None:
            order.append("recovery")
            await h.runtime.request_recovery(signal)

        def refresh(changed: StreamKey) -> None:
            order.append("sync")
            h.runtime.on_open_interest_changed(changed)
            assert _ready(h) == [False, True]

        def append(s: StreamKey, t: int) -> bool:
            if append_result == "error":
                raise TimeoutError("OI unavailable")
            return bool(append_result)

        publication = OpenInterestPublication(
            streams=(stream,),
            append=append,
            supervisor=h.supervisor,
            request_recovery=recovery,
            on_open_interest_changed=refresh,
        )
        assert not await publication.prepare(bar_notification(stream, 600_000))
        assert h.supervisor.facts(stream).last_open_interest_open_time_ms == 0
        if append_result is True:
            assert order == ["recovery", "sync"]
            assert set(h.reader.reads) == {stream}
            assert h.supervisor.facts(stream).recovery_pending
        else:
            assert order == []
            assert h.reader.reads == []
            assert _ready(h) == [True, True]

    asyncio.run(scenario())
