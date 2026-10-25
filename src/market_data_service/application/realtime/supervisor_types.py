"""Realtime supervision facts and policies."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.identity import StreamKey


class RealtimeStreamStatus(StrEnum):
    EXPECTED = "expected"
    SUBSCRIBED = "subscribed"
    LIVE = "live"
    STALE = "stale"
    DISCONNECTED = "disconnected"
    RECOVERY_REQUIRED = "recovery_required"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class StalePolicy:
    grace_ms: int = 15_000

    def __post_init__(self) -> None:
        if self.grace_ms < 0:
            raise ValueError("stale grace_ms must be non-negative")


@dataclass(frozen=True, slots=True)
class RealtimeStreamFacts:
    stream: StreamKey
    status: RealtimeStreamStatus = RealtimeStreamStatus.EXPECTED
    subscription_active: bool = False
    last_transport_activity_ms: int | None = None
    last_confirmed_observed_at_ms: int | None = None
    last_confirmed_open_time_ms: int | None = None
    last_successful_open_time_ms: int | None = None
    last_checked_expected_open_ms: int | None = None
    pending_expected_open_ms: int | None = None
    last_ingestion_classification: str | None = None
    recovery_pending: bool = False
    recovery_restored: bool = False
    recovery_completed_at_ms: int | None = None
    fatal_error_code: str | None = None
    open_interest_required: bool = False
    last_open_interest_open_time_ms: int | None = None

    @property
    def continuity_frontier_ms(self) -> int | None:
        """Lower of the data-track frontiers; both tracks gate a flagged stream."""

        if not self.open_interest_required:
            return self.last_successful_open_time_ms
        if (
            self.last_successful_open_time_ms is None
            or self.last_open_interest_open_time_ms is None
        ):
            return None
        return min(self.last_successful_open_time_ms, self.last_open_interest_open_time_ms)

    def published_through(self, open_time_ms: int) -> bool:
        """Stream is data-ready and every required track has reached the bar."""

        frontier = self.continuity_frontier_ms
        return self.data_ready and frontier is not None and frontier >= open_time_ms

    @property
    def data_ready(self) -> bool:
        return (
            self.subscription_active
            and self.recovery_restored
            and not self.recovery_pending
            and self.fatal_error_code is None
        )

    @property
    def realtime_live(self) -> bool:
        return (
            self.data_ready
            and self.status is RealtimeStreamStatus.LIVE
            and self.recovery_completed_at_ms is not None
            and self.last_confirmed_observed_at_ms is not None
            and self.last_confirmed_observed_at_ms >= self.recovery_completed_at_ms
        )
