"""Small failure classification for bounded backfill lifecycle decisions."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.ports.market_data_source import RecoverableMarketDataFailure


@dataclass(frozen=True, slots=True)
class BackfillFailureDecision:
    target_state: StreamLifecycleState
    code: str
    detail: str


def classify_backfill_failure(exc: Exception) -> BackfillFailureDecision:
    """Map one backfill failure to a durable operational state."""

    recoverable = isinstance(exc, RecoverableMarketDataFailure)
    return BackfillFailureDecision(
        target_state=(
            StreamLifecycleState.DEGRADED if recoverable else StreamLifecycleState.FAILED
        ),
        code=type(exc).__name__,
        detail=str(exc),
    )
