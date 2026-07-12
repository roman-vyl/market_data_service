"""Mechanical in-memory updates for realtime supervision."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace

from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome
from market_data_service.application.realtime.supervisor_types import (
    RealtimeStreamFacts,
    RealtimeStreamStatus,
)
from market_data_service.domain.identity import StreamKey


class RealtimeSupervisorState:
    def __init__(
        self,
        streams: Sequence[StreamKey],
        initial_latest_open_time_ms: Mapping[StreamKey, int | None] | None = None,
    ) -> None:
        expected = tuple(streams)
        if not expected:
            raise ValueError("at least one realtime stream is required")
        initial = initial_latest_open_time_ms or {}
        self._facts = {
            stream: RealtimeStreamFacts(
                stream=stream,
                last_successful_open_time_ms=initial.get(stream),
            )
            for stream in expected
        }

    def subscribe(self, stream: StreamKey, observed_at_ms: int) -> None:
        facts = self._facts[stream]
        self._facts[stream] = replace(
            facts,
            status=RealtimeStreamStatus.SUBSCRIBED,
            subscription_active=True,
            last_transport_activity_ms=observed_at_ms,
        )

    def observe_candle(self, event: CandleObserved) -> None:
        facts = self._facts[event.stream]
        candle = event.candle
        self._facts[event.stream] = replace(
            facts,
            last_transport_activity_ms=candle.observed_at_ms,
            last_confirmed_observed_at_ms=(
                candle.observed_at_ms if candle.confirmed else facts.last_confirmed_observed_at_ms
            ),
            last_confirmed_open_time_ms=(
                candle.open_time_ms if candle.confirmed else facts.last_confirmed_open_time_ms
            ),
        )

    def record_success(self, outcome: RealtimeIngestionOutcome) -> int | None:
        facts = self._facts[outcome.stream]
        previous = facts.last_successful_open_time_ms
        self._facts[outcome.stream] = replace(
            facts,
            status=RealtimeStreamStatus.LIVE,
            last_successful_open_time_ms=max(
                previous or outcome.open_time_ms,
                outcome.open_time_ms,
            ),
            last_ingestion_classification=outcome.classification.value,
        )
        return previous

    def record_rejected(self, outcome: RealtimeIngestionOutcome) -> None:
        facts = self._facts[outcome.stream]
        self._facts[outcome.stream] = replace(
            facts,
            status=RealtimeStreamStatus.RECOVERY_REQUIRED,
            last_ingestion_classification=outcome.classification.value,
            recovery_pending=True,
            recovery_restored=False,
        )

    def record_failed(self, outcome: RealtimeIngestionOutcome) -> None:
        facts = self._facts[outcome.stream]
        self._facts[outcome.stream] = replace(
            facts,
            status=RealtimeStreamStatus.FAILED,
            last_ingestion_classification=outcome.classification.value,
            fatal_error_code=outcome.error_code or "realtime_ingestion_failed",
        )

    def require_recovery(self, stream: StreamKey, status: RealtimeStreamStatus) -> None:
        facts = self._facts[stream]
        self._facts[stream] = replace(
            facts,
            status=status,
            recovery_pending=True,
            recovery_restored=False,
        )

    def record_recovery_result(
        self,
        stream: StreamKey,
        *,
        restored: bool,
        fatal: bool,
        completed_at_ms: int,
        restored_through_open_time_ms: int | None,
    ) -> None:
        facts = self._facts[stream]
        if fatal:
            self._facts[stream] = replace(
                facts,
                status=RealtimeStreamStatus.FAILED,
                recovery_pending=False,
                recovery_restored=False,
                fatal_error_code="recovery_fatal_failure",
            )
            return
        self._facts[stream] = replace(
            facts,
            status=(
                RealtimeStreamStatus.SUBSCRIBED
                if restored and facts.subscription_active
                else RealtimeStreamStatus.RECOVERY_REQUIRED
            ),
            recovery_pending=not restored,
            recovery_restored=restored,
            recovery_completed_at_ms=completed_at_ms if restored else None,
            last_successful_open_time_ms=(
                max(
                    facts.last_successful_open_time_ms
                    or restored_through_open_time_ms
                    or 0,
                    restored_through_open_time_ms or 0,
                )
                if restored
                else facts.last_successful_open_time_ms
            ),
        )

    def mark_disconnected(self, observed_at_ms: int) -> None:
        for stream, facts in tuple(self._facts.items()):
            self._facts[stream] = replace(
                facts,
                status=RealtimeStreamStatus.DISCONNECTED,
                subscription_active=False,
                last_transport_activity_ms=observed_at_ms,
                recovery_restored=False,
            )

    def mark_stopped(self, observed_at_ms: int) -> None:
        for stream, facts in tuple(self._facts.items()):
            self._facts[stream] = replace(
                facts,
                status=RealtimeStreamStatus.STOPPED,
                subscription_active=False,
                last_transport_activity_ms=observed_at_ms,
            )

    def touch_all(self, observed_at_ms: int) -> None:
        for stream, facts in tuple(self._facts.items()):
            self._facts[stream] = replace(facts, last_transport_activity_ms=observed_at_ms)

    def touch_subscribed(self, observed_at_ms: int) -> None:
        for stream, facts in tuple(self._facts.items()):
            if facts.subscription_active:
                self._facts[stream] = replace(
                    facts, last_transport_activity_ms=observed_at_ms
                )

    def facts(self, stream: StreamKey) -> RealtimeStreamFacts:
        return self._facts[stream]

    def all_facts(self) -> tuple[RealtimeStreamFacts, ...]:
        return tuple(self._facts.values())
