from __future__ import annotations

from market_data_service.application.realtime.events import (
    CandleObserved,
    Disconnected,
    HeartbeatObserved,
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
        stale_policy=StalePolicy(grace_ms=0),
    )
    supervisor.observe_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT", "kline.5.ETHUSDT"), observed_at_ms=1_000)
    )

    supervisor.observe_event(CandleObserved(btc, _candle(btc, 0, 2_000)))
    assert (
        supervisor.observe_outcome(
            RealtimeIngestionOutcome(btc, 0, RealtimeIngestionClassification.COMMITTED)
        )
        == ()
    )
    supervisor.observe_event(CandleObserved(btc, _candle(btc, 120_000, 3_000)))
    signals = supervisor.observe_outcome(
        RealtimeIngestionOutcome(btc, 120_000, RealtimeIngestionClassification.COMMITTED)
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
    now[0] = 400_000
    stale = supervisor.detect_stale(now_ms=400_000)
    assert {signal.stream for signal in stale} == {btc}
    assert stale[0].reason is RecoveryReason.STALE


def test_disconnect_emits_recovery_only_after_resubscribe_and_failed_is_fatal() -> None:
    btc = _stream()
    supervisor = RealtimeSupervisor((btc,), {"kline.1.BTCUSDT": btc}, lambda: 10_000)
    supervisor.observe_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=1_000))
    assert supervisor.observe_event(Disconnected(1006, "lost", 2_000)) == ()
    signals = supervisor.observe_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=3_000)
    )
    assert signals[0].reason is RecoveryReason.DISCONNECT

    assert (
        supervisor.observe_outcome(
            RealtimeIngestionOutcome(
                btc,
                0,
                RealtimeIngestionClassification.FAILED,
                error_code="SqliteError",
            )
        )
        == ()
    )
    facts = supervisor.facts(btc)
    assert facts.status is RealtimeStreamStatus.FAILED
    assert facts.fatal_error_code == "SqliteError"


def test_durable_progress_synchronization_moves_forward_only() -> None:
    stream = _stream()
    supervisor = RealtimeSupervisor(
        (stream,),
        {"kline.1.BTCUSDT": stream},
        lambda: 1_000,
        initial_latest_open_time_ms={stream: 60_000},
    )

    supervisor.synchronize_successful_open_time(stream, 240_000)
    supervisor.synchronize_successful_open_time(stream, 120_000)

    assert supervisor.facts(stream).last_successful_open_time_ms == 240_000


STEP = 300_000
T0 = 1_799_971_200_000  # aligned to every supported grid (multiple of one day)
GRACE = 15_000


def _expected_bar_supervisor(
    timeframe: str = "5m", last_open: int | None = T0
) -> tuple[RealtimeSupervisor, StreamKey, list[int]]:
    stream = _stream(timeframe=timeframe)
    now = [T0]
    supervisor = RealtimeSupervisor(
        (stream,),
        {"kline.5.BTCUSDT": stream},
        lambda: now[0],
        stale_policy=StalePolicy(grace_ms=GRACE),
        initial_latest_open_time_ms={stream: last_open},
    )
    supervisor.observe_event(SubscriptionConfirmed(("kline.5.BTCUSDT",), observed_at_ms=T0))
    return supervisor, stream, now


def _ws_bar(supervisor: RealtimeSupervisor, stream: StreamKey, open_ms: int) -> None:
    supervisor.observe_outcome(
        RealtimeIngestionOutcome(stream, open_ms, RealtimeIngestionClassification.COMMITTED)
    )


def test_default_grace_is_fifteen_seconds() -> None:
    assert StalePolicy().grace_ms == 15_000


def test_websocket_bar_within_grace_emits_no_signal() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    for bar in range(1, 6):
        now[0] = T0 + (bar + 1) * STEP + 2_000
        _ws_bar(supervisor, stream, T0 + bar * STEP)
        for second in range(0, STEP // 1_000):
            now[0] = T0 + (bar + 1) * STEP + 2_000 + second * 1_000
            assert supervisor.detect_stale() == ()
    assert supervisor.facts(stream).last_checked_expected_open_ms is None


def test_silent_websocket_gives_one_signal_per_missed_bar_over_25_minutes() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    signals: list[tuple[int, int | None, int | None]] = []
    # 1 s ticks for 25 minutes; the last committed bar is T0 and the WebSocket stays silent.
    for second in range(0, 25 * 60):
        now[0] = T0 + STEP + second * 1_000
        for signal in supervisor.detect_stale():
            assert signal.reason is RecoveryReason.STALE
            signals.append(
                (
                    now[0],
                    signal.suspected_start_time_ms,
                    supervisor.facts(stream).pending_expected_open_ms,
                )
            )
            # Restored with attempted=0 committed=0: the durable state does not move.
            supervisor.record_recovery_result(
                stream,
                restored=True,
                restored_through_open_time_ms=supervisor.facts(stream).last_successful_open_time_ms,
            )

    # Bar k closes at T0 + (k + 1) * STEP and is checked at close + grace: exactly once each.
    assert signals == [
        (T0 + 2 * STEP + GRACE, T0 + STEP, T0 + STEP),
        (T0 + 3 * STEP + GRACE, T0 + STEP, T0 + 2 * STEP),
        (T0 + 4 * STEP + GRACE, T0 + STEP, T0 + 3 * STEP),
        (T0 + 5 * STEP + GRACE, T0 + STEP, T0 + 4 * STEP),
    ]


def test_incomplete_or_failed_recovery_keeps_original_pending_bar() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    first = supervisor.detect_stale()
    assert len(first) == 1
    expected_e1 = supervisor.facts(stream).pending_expected_open_ms
    assert expected_e1 == T0 + STEP

    supervisor.record_recovery_result(stream, restored=False)
    now[0] = T0 + 5 * STEP + GRACE
    assert supervisor.detect_stale() == ()
    facts = supervisor.facts(stream)
    assert facts.pending_expected_open_ms == expected_e1
    assert facts.last_checked_expected_open_ms is None

    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + 4 * STEP
    )
    facts = supervisor.facts(stream)
    assert facts.last_checked_expected_open_ms == expected_e1
    assert facts.pending_expected_open_ms is None


def test_boundary_crossed_during_recovery_marks_original_bar_only() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    assert len(supervisor.detect_stale()) == 1  # E1 = T0 + STEP
    now[0] = T0 + 3 * STEP + GRACE  # E2 = T0 + 2*STEP is due while E1 recovers
    assert supervisor.detect_stale() == ()
    # Recovery restored through E1 only: E2 is still missing.
    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + STEP
    )
    assert supervisor.facts(stream).last_checked_expected_open_ms == T0 + STEP
    signals = supervisor.detect_stale()
    assert len(signals) == 1
    assert supervisor.facts(stream).pending_expected_open_ms == T0 + 2 * STEP
    assert supervisor.detect_stale() == ()


def test_boundary_crossed_and_restored_recovery_covered_next_bar() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    assert len(supervisor.detect_stale()) == 1
    now[0] = T0 + 3 * STEP + GRACE
    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + 2 * STEP
    )
    assert supervisor.detect_stale() == ()


def test_long_idle_emits_one_signal_for_the_current_expected_bar() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 100 * STEP + GRACE
    signals = supervisor.detect_stale()
    assert len(signals) == 1
    assert signals[0].suspected_start_time_ms == T0 + STEP
    assert supervisor.facts(stream).pending_expected_open_ms == T0 + 99 * STEP
    assert supervisor.detect_stale() == ()
    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + 99 * STEP
    )
    assert supervisor.detect_stale() == ()


def test_hole_rest_cannot_fill_is_not_rechecked_for_same_bar() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    assert len(supervisor.detect_stale()) == 1
    supervisor.record_recovery_result(stream, restored=True, restored_through_open_time_ms=T0)
    for second in range(0, 280):
        now[0] = T0 + 2 * STEP + GRACE + second * 1_000
        assert supervisor.detect_stale() == ()
    now[0] = T0 + 3 * STEP + GRACE
    assert len(supervisor.detect_stale()) == 1


def test_late_websocket_candle_after_recovery_does_not_signal() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    assert len(supervisor.detect_stale()) == 1
    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + STEP
    )
    now[0] += 30_000
    supervisor.observe_outcome(
        RealtimeIngestionOutcome(stream, T0 + STEP, RealtimeIngestionClassification.DUPLICATE)
    )
    assert supervisor.detect_stale() == ()


def test_each_timeframe_uses_its_own_grid() -> None:
    grids = (("5m", 300_000), ("1h", 3_600_000), ("4h", 14_400_000), ("1d", 86_400_000))
    for timeframe, step in grids:
        supervisor, stream, now = _expected_bar_supervisor(timeframe)
        now[0] = T0 + 2 * step + GRACE - 1
        assert supervisor.detect_stale() == (), timeframe
        now[0] = T0 + 2 * step + GRACE
        signals = supervisor.detect_stale()
        assert len(signals) == 1, timeframe
        assert signals[0].suspected_start_time_ms == T0 + step
        assert supervisor.facts(stream).pending_expected_open_ms == T0 + step


def test_stream_without_anchor_emits_no_signal() -> None:
    supervisor, _, now = _expected_bar_supervisor(last_open=None)
    now[0] = T0 + 100 * STEP
    assert supervisor.detect_stale() == ()


def test_heartbeat_does_not_postpone_stale_and_recovery_does_not_make_stream_live() -> None:
    supervisor, stream, now = _expected_bar_supervisor()
    now[0] = T0 + 2 * STEP + GRACE
    supervisor.observe_event(HeartbeatObserved(now[0]))
    assert len(supervisor.detect_stale()) == 1
    supervisor.record_recovery_result(
        stream, restored=True, restored_through_open_time_ms=T0 + STEP
    )
    facts = supervisor.facts(stream)
    assert facts.data_ready is True
    assert facts.realtime_live is False
