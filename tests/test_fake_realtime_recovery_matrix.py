from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

from tests.fake_bybit_api import FakeBybitApi, FakeBybitState

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.bybit.websocket import BybitTopicMap, BybitWebSocketAdapter
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.realtime.events import (
    CandleObserved,
    Disconnected,
    RecoveryReason,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.recovery import RealtimeRecoveryCoordinator
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.config import load_market_config
from market_data_service.domain.timeframes import get_timeframe


@dataclass
class MutableClock:
    value: int

    def now_ms(self) -> int:
        return self.value


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "markets.toml"
    path.write_text(
        '''
schema_version = 1
[source]
venue = "bybit"
category = "linear"
[[instruments]]
ticker = "BTCUSDT.P"
exchange_symbol = "BTCUSDT"
enabled = true
canonical_timeframes = ["1m"]
history_policy = "full_available"
[[instruments]]
ticker = "ETHUSDT.P"
exchange_symbol = "ETHUSDT"
enabled = true
canonical_timeframes = ["1m", "5m"]
history_policy = "full_available"
''',
        encoding="utf-8",
    )
    return path


def _frame(topic: str, *, open_time_ms: int, step_ms: int) -> str:
    return json.dumps(
        {
            "topic": topic,
            "data": [
                {
                    "start": open_time_ms,
                    "end": open_time_ms + step_ms - 1,
                    "interval": topic.split(".")[1],
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": "10",
                    "confirm": True,
                }
            ],
        }
    )


def test_fake_websocket_rest_recovery_matrix(tmp_path: Path) -> None:
    asyncio.run(_scenario(tmp_path))


async def _scenario(tmp_path: Path) -> None:
    config = load_market_config(_write_config(tmp_path))
    topic_map = BybitTopicMap.from_config(config)
    adapter = BybitWebSocketAdapter(topic_map)
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    coverage_by_ticker = {item.instrument.ticker: item for item in config.enabled_instruments}
    for stream in config.enabled_streams:
        register_stream(
            database,
            stream,
            exchange_symbol=coverage_by_ticker[stream.instrument.ticker].exchange_symbol,
            now_ms=1,
        )

    state = FakeBybitState()
    for stream in config.enabled_streams:
        coverage = coverage_by_ticker[stream.instrument.ticker]
        timeframe = get_timeframe(stream.timeframe)
        state.seed_stream(
            coverage.exchange_symbol,
            timeframe.bybit_interval,
            start_ms=0,
            count=20,
            step_ms=timeframe.duration_ms,
            base=100 if stream.instrument.ticker.startswith("BTC") else 500,
        )

    clock = MutableClock(600_000)
    with FakeBybitApi(state) as api:
        source = BybitRestCandleSource(
            exchange_symbols=config.exchange_symbols,
            base_url=api.base_url,
        )

        def uow_factory() -> SqliteUnitOfWork:
            return SqliteUnitOfWork(database)

        importer = ImportHistoricalWindow(source, uow_factory, clock)
        backfill = BackfillStreamHistory(importer, uow_factory, clock)
        auditor = AuditStreamContinuity(uow_factory)
        repair = RepairStreamGaps(auditor, importer, uow_factory, clock)
        coordinator = RealtimeRecoveryCoordinator(
            backfill=backfill,
            auditor=auditor,
            repair=repair,
            unit_of_work_factory=uow_factory,
            now_ms=clock.now_ms,
        )
        handler = RealtimeCandleHandler(IngestObservedCandle(uow_factory), clock.now_ms)

        for stream in config.enabled_streams:
            step_ms = get_timeframe(stream.timeframe).duration_ms
            assert backfill.execute(
                BackfillStreamRequest(stream, 0, step_ms * 2, max_windows=1)
            ).reached_end

        with uow_factory() as unit_of_work:
            initial_latest = {
                stream: unit_of_work.get_stream_state(stream).latest_committed_open_time_ms
                for stream in config.enabled_streams
            }
        supervisor = RealtimeSupervisor(
            config.enabled_streams,
            {topic: topic_map.resolve(topic) for topic in topic_map.topics},
            clock.now_ms,
            initial_latest_open_time_ms=initial_latest,
        )

        subscribe = adapter.parse(
            json.dumps(
                {
                    "op": "subscribe",
                    "success": True,
                    "data": {"successTopics": list(topic_map.topics)},
                }
            ),
            observed_at_ms=clock.now_ms(),
        )
        for event in subscribe:
            assert isinstance(event, SubscriptionConfirmed)
            supervisor.observe_event(event)

        btc = next(stream for stream in config.enabled_streams if stream.timeframe == "1m")
        eth = next(stream for stream in config.enabled_streams if stream.timeframe == "5m")
        btc_topic = next(topic for topic in topic_map.topics if "BTCUSDT" in topic)
        eth_topic = next(topic for topic in topic_map.topics if topic_map.resolve(topic) == eth)

        btc_event = adapter.parse(
            _frame(btc_topic, open_time_ms=180_000, step_ms=60_000),
            observed_at_ms=240_000,
        )[0]
        assert isinstance(btc_event, CandleObserved)
        supervisor.observe_event(btc_event)
        btc_outcome = handler.handle(btc_event)
        assert btc_outcome is not None
        btc_signals = supervisor.observe_outcome(btc_outcome)
        assert btc_signals[0].reason is RecoveryReason.SEQUENCE_DISCONTINUITY
        assert btc_signals[0].suspected_start_time_ms == 120_000

        eth_event = adapter.parse(
            _frame(eth_topic, open_time_ms=600_000, step_ms=300_000),
            observed_at_ms=900_000,
        )[0]
        assert isinstance(eth_event, CandleObserved)
        supervisor.observe_event(eth_event)
        eth_outcome = handler.handle(eth_event)
        assert eth_outcome is not None
        assert supervisor.observe_outcome(eth_outcome) == ()

        clock.value = 240_000
        btc_recovery = await coordinator.execute(
            RealtimeRecoveryRequest(
                btc_signals[0], max_backfill_windows=1, max_repair_windows=1
            )
        )
        assert btc_recovery.classification is RecoveryClassification.RESTORED
        supervisor.record_recovery_result(
            btc,
            restored=True,
            restored_through_open_time_ms=btc_recovery.restored_through_open_time_ms,
        )
        assert supervisor.facts(eth).recovery_pending is False

        supervisor.observe_event(Disconnected(1006, "network", 910_000))
        resubscribe_signals = supervisor.observe_event(
            SubscriptionConfirmed(tuple(topic_map.topics), observed_at_ms=920_000)
        )
        assert {signal.stream for signal in resubscribe_signals} == set(config.enabled_streams)

        clock.value = 900_000
        results = await asyncio.gather(
            *(
                coordinator.execute(
                    RealtimeRecoveryRequest(
                        signal, max_backfill_windows=2, max_repair_windows=1
                    )
                )
                for signal in resubscribe_signals
            )
        )
        assert all(result.classification is RecoveryClassification.RESTORED for result in results)
        for result in results:
            supervisor.record_recovery_result(
                result.stream,
                restored=True,
                restored_through_open_time_ms=result.restored_through_open_time_ms,
            )
        assert not any(facts.realtime_ready for facts in supervisor.all_facts())

        for stream in config.enabled_streams:
            topic = next(topic for topic in topic_map.topics if topic_map.resolve(topic) == stream)
            step_ms = get_timeframe(stream.timeframe).duration_ms
            with uow_factory() as unit_of_work:
                latest = unit_of_work.get_stream_state(stream).latest_committed_open_time_ms
            assert latest is not None
            latest_open = latest + step_ms
            clock.value = latest_open + step_ms
            fresh_event = adapter.parse(
                _frame(topic, open_time_ms=latest_open, step_ms=step_ms),
                observed_at_ms=clock.now_ms(),
            )[0]
            assert isinstance(fresh_event, CandleObserved)
            supervisor.observe_event(fresh_event)
            fresh_outcome = handler.handle(fresh_event)
            assert fresh_outcome is not None
            supervisor.observe_outcome(fresh_outcome)
        assert all(facts.realtime_ready for facts in supervisor.all_facts())

        with uow_factory() as unit_of_work:
            assert unit_of_work.get_candle(btc, 120_000) is not None
            assert unit_of_work.get_candle(eth, 600_000) is not None
