"""Fetch one bounded historical window and ingest it atomically."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain.candles import ObservedCandle
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
    unexpected: int = 0


class Clock(Protocol):
    def now_ms(self) -> int: ...


class ImportHistoricalWindow:
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
        unexpected = 0
        with self._unit_of_work_factory() as unit_of_work:
            for candle in candles:
                if candle.stream != stream or not window.contains(candle.open_time_ms):
                    unexpected += 1
                    _record_unexpected_historical_candle(
                        unit_of_work,
                        requested_stream=stream,
                        requested_window=window,
                        candle=candle,
                        created_at_ms=self._clock.now_ms(),
                    )
                    continue
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
            unexpected=unexpected,
        )


def _record_unexpected_historical_candle(
    unit_of_work: CanonicalStorageUnitOfWork,
    *,
    requested_stream: StreamKey,
    requested_window: TimeWindow,
    candle: ObservedCandle,
    created_at_ms: int,
) -> None:
    open_time_ms = candle.open_time_ms
    end_ms = max(open_time_ms + 1, candle.close_time_ms + 1)
    unit_of_work.record_quarantine(
        stream=requested_stream,
        start_ms=max(0, open_time_ms),
        end_ms=max(max(0, open_time_ms) + 1, end_ms),
        reason_code="unexpected_historical_candle",
        detail=(
            f"requested_stream={requested_stream.canonical_id} "
            f"requested_window=[{requested_window.start_ms}, {requested_window.end_ms}) "
            f"observed_stream={candle.stream.canonical_id}"
        ),
        payload_json=None,
        created_at_ms=created_at_ms,
    )
