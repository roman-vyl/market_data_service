"""A stale public projection cannot bypass canonical consumer gates."""

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.consumer_candle_reader import SqliteConsumerCandleReader
from market_data_service.adapters.sqlite.stream_state_reader import SqliteStreamStateReader
from market_data_service.application.consumer_read import CandleRangeRequest, GetCandleRange
from market_data_service.application.consumer_read.errors import StreamNotReady
from market_data_service.application.realtime.events import Disconnected, SubscriptionConfirmed
from market_data_service.application.realtime.recovery_state import RealtimeRecoveryStateRecorder
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
    bar_notification,
)
from market_data_service.runtime.open_interest_publication import OpenInterestPublication
from market_data_service.runtime.status import RuntimeStatusStore


def test_stale_ready_cannot_admit_ranges_or_send_oi(tmp_path):
    root = tmp_path
    p = Path(root) / "state.sqlite3"
    initialize_database(p)
    stream = StreamKey(InstrumentKey("TESTUSDT.P"), "5m")
    topic = "kline.5.TESTUSDT"
    register_stream(p, stream, exchange_symbol="TESTUSDT", now_ms=1)

    def factory():
        return SqliteUnitOfWork(p)

    with factory() as u:
        u.save_stream_state(
            replace(
                u.get_stream_state(stream),
                state=StreamLifecycleState.READY,
                earliest_available_open_time_ms=0,
                latest_committed_open_time_ms=0,
            )
        )
        u.commit()
    old = SqliteStreamStateReader(p).get_stream_state(stream)
    supervisor = RealtimeSupervisor(
        (stream,),
        {topic: stream},
        lambda: 315000,
        initial_latest_open_time_ms={stream: 0},
        initial_open_interest_open_time_ms={stream: 0},
    )
    supervisor.observe_event(SubscriptionConfirmed((topic,), 315000))
    supervisor.record_recovery_result(stream, restored=True)
    oldfacts = supervisor.facts(stream)
    assert oldfacts.data_ready
    supervisor.observe_event(Disconnected(None, None, 315000))
    supervisor.observe_event(SubscriptionConfirmed((topic,), 315000))
    recovery = RealtimeRecoveryStateRecorder(factory, lambda: 315000)
    recovery.mark_unavailable(stream, reason="disconnect")
    recovery.ensure_auditing(stream)
    stale = RuntimeStatusStore((stream,))
    stale.update_stream(old, oldfacts)
    assert stale.ready
    config = ValidatedMarketConfig(
        1,
        MarketSourceConfig("bybit", "linear"),
        (
            InstrumentCoverage(
                stream.instrument, "TESTUSDT", True, ("5m",), HistoryPolicy.FULL_AVAILABLE
            ),
        ),
    )
    with pytest.raises(StreamNotReady):
        GetCandleRange(config, SqliteConsumerCandleReader(p)).execute(
            CandleRangeRequest("TESTUSDT.P", "5m", 0, 300000)
        )

    class Notifier:
        def __init__(self):
            self.calls = []

        def send(self, n):
            self.calls.append(n)

    async def scenario():
        notifier = Notifier()
        stop = asyncio.Event()

        async def request(_s):
            raise AssertionError("unexpected new recovery requirement")

        publication = OpenInterestPublication(
            streams=(stream,),
            append=lambda s, t: True,
            supervisor=supervisor,
            request_recovery=request,
            on_open_interest_changed=lambda s: None,
        )

        async def prepare(n):
            allowed = await publication.prepare(n)
            stop.set()
            return allowed

        worker = CommittedBarNotificationWorker(notifier, 4, prepare=prepare)
        await worker.enqueue(bar_notification(stream, 0))
        await worker.run(stop)
        assert not notifier.calls

    asyncio.run(scenario())
    status = RuntimeStatusStore((stream,))
    status.update_stream(old, supervisor.facts(stream))
    assert not status.ready
