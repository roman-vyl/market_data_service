"""Fetch one bounded historical window and pass every candle through ingestion."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain.classification import IngestionClassification
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.market_data_source import HistoricalCandleSource
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


@dataclass(frozen=True, slots=True)
class ImportWindowResult:
    observed: int
    committed: int
    duplicates: int
    corrected: int
    rejected: int


class Clock(Protocol):
    def now_ms(self) -> int: ...


class ImportHistoricalWindow:
    """Small orchestration boundary for one REST window."""

    def __init__(
        self,
        source: HistoricalCandleSource,
        ingest: IngestObservedCandle,
        clock: Clock,
    ) -> None:
        self._source = source
        self._ingest = ingest
        self._clock = clock

    def execute(self, stream: StreamKey, window: TimeWindow) -> ImportWindowResult:
        observed_at_ms = self._clock.now_ms()
        candles = self._source.fetch_closed_candles(
            stream,
            window,
            observed_at_ms=observed_at_ms,
        )
        counts = {classification: 0 for classification in IngestionClassification}
        for candle in candles:
            result = self._ingest.execute(candle, committed_at_ms=self._clock.now_ms())
            counts[result.classification] += 1
        rejected = sum(
            counts[item]
            for item in (
                IngestionClassification.REJECTED_INVALID,
                IngestionClassification.REJECTED_UNCONFIRMED,
                IngestionClassification.REJECTED_UNCONFIGURED,
            )
        )
        return ImportWindowResult(
            observed=len(candles),
            committed=counts[IngestionClassification.COMMITTED],
            duplicates=counts[IngestionClassification.DUPLICATE],
            corrected=counts[IngestionClassification.CORRECTED],
            rejected=rejected,
        )


class ImportHistoricalWindowBatch:
    """Import one REST window inside a single storage transaction."""

    def __init__(
        self,
        source: HistoricalCandleSource,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
    ) -> None:
        self._source = source
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock

    def execute(self, stream: StreamKey, window: TimeWindow) -> ImportWindowResult:
        observed_at_ms = self._clock.now_ms()
        candles = self._source.fetch_closed_candles(
            stream,
            window,
            observed_at_ms=observed_at_ms,
        )
        counts = {classification: 0 for classification in IngestionClassification}
        with self._unit_of_work_factory() as unit_of_work:
            for candle in candles:
                result = IngestObservedCandle.execute_in_unit_of_work(
                    unit_of_work,
                    candle,
                    committed_at_ms=self._clock.now_ms(),
                )
                counts[result.classification] += 1
            unit_of_work.commit()
        rejected = sum(
            counts[item]
            for item in (
                IngestionClassification.REJECTED_INVALID,
                IngestionClassification.REJECTED_UNCONFIRMED,
                IngestionClassification.REJECTED_UNCONFIGURED,
            )
        )
        return ImportWindowResult(
            observed=len(candles),
            committed=counts[IngestionClassification.COMMITTED],
            duplicates=counts[IngestionClassification.DUPLICATE],
            corrected=counts[IngestionClassification.CORRECTED],
            rejected=rejected,
        )
