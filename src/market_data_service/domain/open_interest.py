"""Open interest observations, acceptance rules and track progress facts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from market_data_service.domain.classification import IngestionClassification
from market_data_service.domain.decimal_values import (
    DecimalInput,
    InvalidDecimalValue,
    decimal_to_canonical_text,
    parse_decimal,
)
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe

OPEN_INTEREST_TRACK = "open_interest"
OPEN_INTEREST_TIMEFRAME = "5m"


@dataclass(frozen=True, slots=True)
class ObservedOpenInterest:
    """One vendor open interest point on the stream grid.

    Bybit publishes the point at ``open_time_ms`` as a snapshot taken at that
    moment, so it is stored by the same open time as the bar it opens.
    """

    stream: StreamKey
    open_time_ms: int
    open_interest: DecimalInput
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class CanonicalOpenInterest:
    stream: StreamKey
    open_time_ms: int
    open_interest: str
    committed_at_ms: int


@dataclass(frozen=True, slots=True)
class TrackProgress:
    """Durable progress of one non-candle historical track of a stream."""

    stream: StreamKey
    track: str
    earliest_available_open_time_ms: int | None = None
    discovery_next_open_time_ms: int | None = None
    latest_committed_open_time_ms: int | None = None
    last_audit_at_ms: int | None = None
    last_audit_gap_count: int | None = None
    last_success_at_ms: int | None = None
    last_error_code: str | None = None
    last_error_detail: str | None = None
    updated_at_ms: int = 0

    def __post_init__(self) -> None:
        if (
            self.earliest_available_open_time_ms is not None
            and self.discovery_next_open_time_ms is not None
        ):
            raise ValueError("resolved lower bound cannot coexist with a discovery cursor")


def open_interest_issue(point: ObservedOpenInterest) -> str | None:
    """Return a reason code when the point cannot become canonical."""

    if point.stream.timeframe != OPEN_INTEREST_TIMEFRAME:
        return "open_interest_timeframe"
    if point.open_time_ms < 0:
        return "open_interest_negative_time"
    if point.open_time_ms % get_timeframe(point.stream.timeframe).duration_ms:
        return "open_interest_off_grid"
    try:
        value = parse_decimal(point.open_interest)
    except InvalidDecimalValue:
        return "invalid_open_interest"
    if value < Decimal(0):
        return "invalid_open_interest"
    return None


def canonical_open_interest(
    point: ObservedOpenInterest,
    *,
    committed_at_ms: int,
) -> CanonicalOpenInterest:
    return CanonicalOpenInterest(
        stream=point.stream,
        open_time_ms=point.open_time_ms,
        open_interest=decimal_to_canonical_text(point.open_interest),
        committed_at_ms=committed_at_ms,
    )


def classify_open_interest(
    incoming: CanonicalOpenInterest,
    existing: CanonicalOpenInterest | None,
) -> IngestionClassification:
    if existing is None:
        return IngestionClassification.COMMITTED
    if existing.open_interest == incoming.open_interest:
        return IngestionClassification.DUPLICATE
    return IngestionClassification.CORRECTED
