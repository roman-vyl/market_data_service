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
    intervals: int = 2
    grace_ms: int = 5_000

    def __post_init__(self) -> None:
        if self.intervals <= 0:
            raise ValueError("stale intervals must be positive")
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
    last_ingestion_classification: str | None = None
    recovery_pending: bool = False
    recovery_restored: bool = False
    recovery_completed_at_ms: int | None = None
    fatal_error_code: str | None = None

    @property
    def realtime_ready(self) -> bool:
        return (
            self.subscription_active
            and self.status is RealtimeStreamStatus.LIVE
            and self.recovery_restored
            and self.recovery_completed_at_ms is not None
            and self.last_confirmed_observed_at_ms is not None
            and self.last_confirmed_observed_at_ms >= self.recovery_completed_at_ms
            and self.fatal_error_code is None
        )
