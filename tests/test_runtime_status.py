from __future__ import annotations

from market_data_service.application.realtime.supervisor_types import (
    RealtimeStreamFacts,
    RealtimeStreamStatus,
)
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState, StreamStateSnapshot
from market_data_service.runtime.status import RuntimeStatusStore


def test_runtime_readiness_requires_durable_and_recovered_subscription() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    store = RuntimeStatusStore((stream,))
    durable = StreamStateSnapshot(stream, StreamLifecycleState.READY)
    realtime = RealtimeStreamFacts(
        stream=stream,
        status=RealtimeStreamStatus.SUBSCRIBED,
        subscription_active=True,
        recovery_restored=True,
        recovery_completed_at_ms=100,
    )
    store.update_stream(durable, realtime)
    assert store.ready is True
    stream_status = store.readiness_document()["streams"][0]
    assert stream_status["data_ready"] is True
    assert stream_status["realtime_live"] is False


def test_runtime_reports_realtime_live_separately_from_data_readiness() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    store = RuntimeStatusStore((stream,))
    durable = StreamStateSnapshot(stream, StreamLifecycleState.READY)
    realtime = RealtimeStreamFacts(
        stream=stream,
        status=RealtimeStreamStatus.LIVE,
        subscription_active=True,
        recovery_restored=True,
        recovery_completed_at_ms=100,
        last_confirmed_observed_at_ms=101,
    )
    store.update_stream(durable, realtime)
    stream_status = store.readiness_document()["streams"][0]
    assert stream_status["ready"] is True
    assert stream_status["data_ready"] is True
    assert stream_status["realtime_live"] is True


def test_runtime_readiness_is_strict_across_streams() -> None:
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    store = RuntimeStatusStore((btc, eth))
    ready_facts = RealtimeStreamFacts(
        stream=btc,
        status=RealtimeStreamStatus.SUBSCRIBED,
        subscription_active=True,
        recovery_restored=True,
        recovery_completed_at_ms=1,
    )
    store.update_stream(StreamStateSnapshot(btc, StreamLifecycleState.READY), ready_facts)
    assert store.ready is False
