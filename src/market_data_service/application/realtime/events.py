"""Transport-neutral realtime events and recovery signals."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.identity import StreamKey


@dataclass(frozen=True, slots=True)
class Connected:
    connected_at_ms: int


@dataclass(frozen=True, slots=True)
class SubscriptionConfirmed:
    topics: tuple[str, ...]
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class HeartbeatObserved:
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class CandleObserved:
    stream: StreamKey
    candle: ObservedCandle


@dataclass(frozen=True, slots=True)
class TransportFailed:
    code: str
    detail: str
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class Disconnected:
    code: int | None
    reason: str | None
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class ReconnectExhausted:
    attempts: int
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class Stopped:
    observed_at_ms: int


class RecoveryReason(StrEnum):
    STARTUP_RECONCILIATION = "startup_reconciliation"
    DISCONNECT = "disconnect"
    STALE = "stale"
    SEQUENCE_DISCONTINUITY = "sequence_discontinuity"
    REJECTED_OBSERVATION = "rejected_observation"


@dataclass(frozen=True, slots=True)
class RecoveryRequired:
    stream: StreamKey
    reason: RecoveryReason
    detected_at_ms: int
    suspected_start_time_ms: int | None = None


RealtimeEvent = (
    Connected
    | SubscriptionConfirmed
    | HeartbeatObserved
    | CandleObserved
    | TransportFailed
    | Disconnected
    | ReconnectExhausted
    | Stopped
)
