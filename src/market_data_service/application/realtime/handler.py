"""Confirmed realtime candle handling through canonical ingestion."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from market_data_service.application.ingest import IngestionResult
from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.classification import IngestionClassification


class CanonicalRealtimeIngestion(Protocol):
    def execute(
        self, candle: ObservedCandle, *, committed_at_ms: int
    ) -> IngestionResult: ...


_CLASSIFICATION_MAP = {
    IngestionClassification.COMMITTED: RealtimeIngestionClassification.COMMITTED,
    IngestionClassification.DUPLICATE: RealtimeIngestionClassification.DUPLICATE,
    IngestionClassification.CORRECTED: RealtimeIngestionClassification.CORRECTED,
    IngestionClassification.REJECTED_INVALID: RealtimeIngestionClassification.REJECTED,
    IngestionClassification.REJECTED_UNCONFIRMED: RealtimeIngestionClassification.REJECTED,
    IngestionClassification.REJECTED_UNCONFIGURED: RealtimeIngestionClassification.REJECTED,
}


class RealtimeCandleHandler:
    """Keep the normal realtime path small and storage-agnostic."""

    def __init__(
        self,
        ingestion: CanonicalRealtimeIngestion,
        now_ms: Callable[[], int],
    ) -> None:
        self._ingestion = ingestion
        self._now_ms = now_ms

    def handle(self, event: CandleObserved) -> RealtimeIngestionOutcome | None:
        candle = event.candle
        if not candle.confirmed:
            return None
        try:
            result = self._ingestion.execute(candle, committed_at_ms=self._now_ms())
        except Exception as exc:
            return RealtimeIngestionOutcome(
                stream=event.stream,
                open_time_ms=candle.open_time_ms,
                classification=RealtimeIngestionClassification.FAILED,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )
        return RealtimeIngestionOutcome(
            stream=event.stream,
            open_time_ms=candle.open_time_ms,
            classification=_CLASSIFICATION_MAP[result.classification],
            issue_codes=result.issue_codes,
        )
