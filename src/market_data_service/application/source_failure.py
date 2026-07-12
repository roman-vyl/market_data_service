"""Shared source-failure classification for historical workflows."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.ports.market_data_source import RecoverableMarketDataFailure


class SourceFailureDisposition(StrEnum):
    RECOVERABLE = "recoverable"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class SourceFailureDecision:
    disposition: SourceFailureDisposition
    target_state: StreamLifecycleState
    code: str
    detail: str


def classify_source_failure(exc: Exception) -> SourceFailureDecision:
    recoverable = isinstance(exc, RecoverableMarketDataFailure)
    return SourceFailureDecision(
        disposition=(
            SourceFailureDisposition.RECOVERABLE
            if recoverable
            else SourceFailureDisposition.FATAL
        ),
        target_state=(
            StreamLifecycleState.DEGRADED if recoverable else StreamLifecycleState.FAILED
        ),
        code=type(exc).__name__,
        detail=str(exc),
    )
