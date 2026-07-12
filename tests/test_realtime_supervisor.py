from __future__ import annotations

from market_data_service.application.realtime.events import (
    CandleObserved,
    Disconnected,
    RecoveryReason,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.application.realtime.supervisor import (
    RealtimeStreamStatus,
    RealtimeSupervisor,
    StalePolicy,
)
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey


def _stream(ticker: str = "BTCUSDT.P", timeframe: str = "1m") -> StreamKey:
    return StreamKey(InstrumentKey(ticker), timeframe)


def _candle(stream: StreamKey, open_time_ms: int, observed_at_ms: int) -> ObservedCandle:
    step = 60_000 if stream.timeframe == "1m" else 300_000
    return ObservedCandle(
        stream=stream,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + step - 1,
        open="100",
        high="102",
        low="99",
        close="101",
        volume="10",
        confirmed=True,
        observed_at_ms=observed_at_ms,
        source=ObservationSource.BYBIT_WEBSOCKET,
    )


def test_sequence_rejected_stale_and_stream_isolation() -> None:
    btc = _stream()
    eth = _stream("ETHUSDT.P", "5m")
    now = [1_000_000]
    supervisor = RealtimeSupervisor(
        (btc, eth),
        {"kline.1.BTCUSDT": btc, "kline.5.ETHUSDT": eth},
        lambda: now[0],
        stale_policy=StalePolicy(intervals=1, grace_ms=0),
    )
    supervisor.observe_event(
        SubscriptionConfirmed(
            ("kline.1.BTCUSDT", "kline.5.ETHUSDT"), observed_at_ms=1_000
        )
    )

    supervisor.observe_event(CandleObserved(btc, _candle(btc, 0, 2_000)))
    assert supervisor.observe_outcome(
        RealtimeIngestionOutcome(
            btc, 0, RealtimeIngestionClassification.COMMITTED
        )
    ) == ()
    supervisor.observe_event(CandleObserved(btc, _candle(btc, 120_000, 3_000)))
    signals = supervisor.observe_outcome(
        RealtimeIngestionOutcome(
            btc, 120_000, RealtimeIngestionClassification.COMMITTED
        )
    )
    assert signals[0].reason is RecoveryReason.SEQUENCE_DISCONTINUITY
    assert signals[0].suspected_start_time_ms == 60_000
    assert supervisor.facts(eth).status is RealtimeStreamStatus.SUBSCRIBED

    rejected = supervisor.observe_outcome(
        RealtimeIngestionOutcome(
            eth, 0, RealtimeIngestionClassification.REJECTED, issue_codes=("invalid_ohlc",)
        )
    )
    assert rejected[0].reason is RecoveryReason.REJECTED_OBSERVATION

    supervisor.record_recovery_result(btc, restored=True)
    supervisor.record_recovery_result(eth, restored=True)
    now[0] = 100_000
    stale = supervisor.detect_stale(now_ms=100_000)
    assert {signal.stream for signal in stale} == {btc}
    assert stale[0].reason is RecoveryReason.STALE


def test_disconnect_emits_recovery_only_after_resubscribe_and_failed_is_fatal() -> None:
    btc = _stream()
    supervisor = RealtimeSupervisor(
        (btc,), {"kline.1.BTCUSDT": btc}, lambda: 10_000
    )
    supervisor.observe_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=1_000)
    )
    assert supervisor.observe_event(Disconnected(1006, "lost", 2_000)) == ()
    signals = supervisor.observe_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=3_000)
    )
    assert signals[0].reason is RecoveryReason.DISCONNECT

    assert supervisor.observe_outcome(
        RealtimeIngestionOutcome(
            btc,
            0,
            RealtimeIngestionClassification.FAILED,
            error_code="SqliteError",
        )
    ) == ()
    facts = supervisor.facts(btc)
    assert facts.status is RealtimeStreamStatus.FAILED
    assert facts.fatal_error_code == "SqliteError"
