"""In-memory per-stream realtime supervision."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace

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
from market_data_service.application.realtime.frontier import extend_frontier
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
from market_data_service.domain.timeframes import align_to_grid, get_timeframe


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
        initial_open_interest_open_time_ms: Mapping[StreamKey, int | None] | None = None,
    ) -> None:
        self._state = RealtimeSupervisorState(
            streams, initial_latest_open_time_ms, initial_open_interest_open_time_ms
        )
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

    def observe_outcome(self, outcome: RealtimeIngestionOutcome) -> tuple[RecoveryRequired, ...]:
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
            self._state.require_recovery(outcome.stream, RealtimeStreamStatus.RECOVERY_REQUIRED)
            return self._signal(
                outcome.stream,
                RecoveryReason.SEQUENCE_DISCONTINUITY,
                previous + step_ms,
            )
        return ()

    def observe_open_interest(
        self, stream: StreamKey, open_time_ms: int
    ) -> tuple[RecoveryRequired, ...]:
        """Live open interest append: the same frontier rule as a live candle commit."""

        previous = self._state.facts(stream).last_open_interest_open_time_ms
        self._state.update(
            stream, last_open_interest_open_time_ms=extend_frontier(previous, open_time_ms, stream)
        )
        step_ms = get_timeframe(stream.timeframe).duration_ms
        if previous is not None and open_time_ms > previous + step_ms:
            self._state.require_recovery(stream, RealtimeStreamStatus.RECOVERY_REQUIRED)
            return self._signal(stream, RecoveryReason.SEQUENCE_DISCONTINUITY, previous + step_ms)
        return ()

    def detect_stale(self, *, now_ms: int | None = None) -> tuple[RecoveryRequired, ...]:
        observed_now = self._now_ms() if now_ms is None else now_ms
        signals: list[RecoveryRequired] = []
        for facts in self._state.all_facts():
            if not facts.subscription_active or facts.recovery_pending or facts.fatal_error_code:
                continue
            frontier_ms = facts.continuity_frontier_ms
            if frontier_ms is None:
                continue
            step_ms = get_timeframe(facts.stream.timeframe).duration_ms
            expected_open_ms = self._expected_open_ms(facts.stream, observed_now)
            if frontier_ms >= expected_open_ms:
                continue
            checked = facts.last_checked_expected_open_ms
            if checked is not None and expected_open_ms <= checked:
                continue
            self._state.require_recovery(
                facts.stream,
                RealtimeStreamStatus.STALE,
                pending_expected_open_ms=expected_open_ms,
            )
            signals.extend(
                self._signal(
                    facts.stream,
                    RecoveryReason.STALE,
                    frontier_ms + step_ms,
                )
            )
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
        return self._as_of_now(self._state.facts(stream))

    def synchronize_successful_open_time(self, stream: StreamKey, open_time_ms: int) -> None:
        self._state.synchronize_successful_open_time(stream, open_time_ms)

    def synchronize_open_interest_open_time(self, stream: StreamKey, open_time_ms: int) -> None:
        previous = self._state.facts(stream).last_open_interest_open_time_ms
        self._state.update(
            stream, last_open_interest_open_time_ms=max(previous or open_time_ms, open_time_ms)
        )

    def all_facts(self) -> tuple[RealtimeStreamFacts, ...]:
        return tuple(self._as_of_now(facts) for facts in self._state.all_facts())

    def _as_of_now(self, facts: RealtimeStreamFacts) -> RealtimeStreamFacts:
        """Readiness is judged against the bar expected now, not at the last stale check."""

        return replace(
            facts, expected_open_time_ms=self._expected_open_ms(facts.stream, self._now_ms())
        )

    def _expected_open_ms(self, stream: StreamKey, now_ms: int) -> int:
        step_ms = get_timeframe(stream.timeframe).duration_ms
        return align_to_grid(now_ms - self._stale_policy.grace_ms, step_ms) - step_ms

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
                self._state.require_recovery(stream, RealtimeStreamStatus.RECOVERY_REQUIRED)
                signals.extend(self._signal(stream, RecoveryReason.DISCONNECT, None))
        return tuple(signals)

    def _signal(
        self,
        stream: StreamKey,
        reason: RecoveryReason,
        suspected_start_time_ms: int | None,
    ) -> tuple[RecoveryRequired, ...]:
        # Recovery starts no later than the bar after proven continuity of every track.
        frontier_ms = self._state.facts(stream).continuity_frontier_ms
        if frontier_ms is not None:
            proven_next = frontier_ms + get_timeframe(stream.timeframe).duration_ms
            suspected_start_time_ms = min(suspected_start_time_ms or proven_next, proven_next)
        return (
            RecoveryRequired(
                stream=stream,
                reason=reason,
                detected_at_ms=self._now_ms(),
                suspected_start_time_ms=suspected_start_time_ms,
            ),
        )
