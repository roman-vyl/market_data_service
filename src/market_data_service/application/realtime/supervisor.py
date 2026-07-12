"""In-memory per-stream realtime supervision."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from market_data_service.application.realtime.events import (
    CandleObserved,
    Connected,
    Disconnected,
    HeartbeatObserved,
    RealtimeEvent,
    ReconnectExhausted,
    RecoveryReason,
    RecoveryRequired,
    Stopped,
    SubscriptionConfirmed,
    TransportFailed,
)
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.application.realtime.supervisor_state import (
    RealtimeSupervisorState,
)
from market_data_service.application.realtime.supervisor_types import (
    RealtimeStreamFacts,
    RealtimeStreamStatus,
    StalePolicy,
)
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe


class RealtimeSupervisor:
    """Report realtime symptoms without performing historical recovery."""

    def __init__(
        self,
        streams: Sequence[StreamKey],
        topic_to_stream: Mapping[str, StreamKey],
        now_ms: Callable[[], int],
        *,
        stale_policy: StalePolicy | None = None,
        initial_latest_open_time_ms: Mapping[StreamKey, int | None] | None = None,
    ) -> None:
        self._state = RealtimeSupervisorState(streams, initial_latest_open_time_ms)
        self._topic_to_stream = dict(topic_to_stream)
        self._now_ms = now_ms
        self._stale_policy = stale_policy or StalePolicy()
        self._disconnected = False

    def observe_event(self, event: RealtimeEvent) -> tuple[RecoveryRequired, ...]:
        if isinstance(event, Connected):
            self._state.touch_all(event.connected_at_ms)
        elif isinstance(event, SubscriptionConfirmed):
            return self._subscriptions_confirmed(event)
        elif isinstance(event, HeartbeatObserved):
            self._state.touch_subscribed(event.observed_at_ms)
        elif isinstance(event, CandleObserved):
            self._state.observe_candle(event)
        elif isinstance(event, (Disconnected, TransportFailed, ReconnectExhausted)):
            self._disconnected = True
            self._state.mark_disconnected(event.observed_at_ms)
        elif isinstance(event, Stopped):
            self._state.mark_stopped(event.observed_at_ms)
        return ()

    def observe_outcome(
        self, outcome: RealtimeIngestionOutcome
    ) -> tuple[RecoveryRequired, ...]:
        if outcome.classification is RealtimeIngestionClassification.FAILED:
            self._state.record_failed(outcome)
            return ()
        if outcome.classification is RealtimeIngestionClassification.REJECTED:
            self._state.record_rejected(outcome)
            return self._signal(
                outcome.stream,
                RecoveryReason.REJECTED_OBSERVATION,
                outcome.open_time_ms,
            )

        previous = self._state.record_success(outcome)
        step_ms = get_timeframe(outcome.stream.timeframe).duration_ms
        if previous is not None and outcome.open_time_ms > previous + step_ms:
            self._state.require_recovery(
                outcome.stream, RealtimeStreamStatus.RECOVERY_REQUIRED
            )
            return self._signal(
                outcome.stream,
                RecoveryReason.SEQUENCE_DISCONTINUITY,
                previous + step_ms,
            )
        return ()

    def detect_stale(self, *, now_ms: int | None = None) -> tuple[RecoveryRequired, ...]:
        observed_now = self._now_ms() if now_ms is None else now_ms
        signals: list[RecoveryRequired] = []
        for facts in self._state.all_facts():
            if not facts.subscription_active or facts.recovery_pending or facts.fatal_error_code:
                continue
            anchor = facts.last_confirmed_observed_at_ms or facts.last_transport_activity_ms
            if anchor is None:
                continue
            step_ms = get_timeframe(facts.stream.timeframe).duration_ms
            timeout_ms = step_ms * self._stale_policy.intervals + self._stale_policy.grace_ms
            if observed_now - anchor <= timeout_ms:
                continue
            suspected_start = (
                None
                if facts.last_successful_open_time_ms is None
                else facts.last_successful_open_time_ms + step_ms
            )
            self._state.require_recovery(facts.stream, RealtimeStreamStatus.STALE)
            signals.extend(self._signal(facts.stream, RecoveryReason.STALE, suspected_start))
        return tuple(signals)

    def record_recovery_result(
        self,
        stream: StreamKey,
        *,
        restored: bool,
        fatal: bool = False,
        restored_through_open_time_ms: int | None = None,
    ) -> None:
        self._state.record_recovery_result(
            stream,
            restored=restored,
            fatal=fatal,
            completed_at_ms=self._now_ms(),
            restored_through_open_time_ms=restored_through_open_time_ms,
        )

    def facts(self, stream: StreamKey) -> RealtimeStreamFacts:
        return self._state.facts(stream)

    def all_facts(self) -> tuple[RealtimeStreamFacts, ...]:
        return self._state.all_facts()

    def _subscriptions_confirmed(
        self, event: SubscriptionConfirmed
    ) -> tuple[RecoveryRequired, ...]:
        restored_after_disconnect = self._disconnected
        self._disconnected = False
        signals: list[RecoveryRequired] = []
        for topic in event.topics:
            stream = self._topic_to_stream[topic]
            self._state.subscribe(stream, event.observed_at_ms)
            if restored_after_disconnect:
                self._state.require_recovery(
                    stream, RealtimeStreamStatus.RECOVERY_REQUIRED
                )
                signals.extend(self._signal(stream, RecoveryReason.DISCONNECT, None))
        return tuple(signals)

    def _signal(
        self,
        stream: StreamKey,
        reason: RecoveryReason,
        suspected_start_time_ms: int | None,
    ) -> tuple[RecoveryRequired, ...]:
        return (
            RecoveryRequired(
                stream=stream,
                reason=reason,
                detected_at_ms=self._now_ms(),
                suspected_start_time_ms=suspected_start_time_ms,
            ),
        )
