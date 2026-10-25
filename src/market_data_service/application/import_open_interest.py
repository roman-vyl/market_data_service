"""Fetch one bounded open interest window and accept it atomically."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Protocol

from market_data_service.application.import_window import ImportWindowResult
from market_data_service.domain.classification import IngestionClassification
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import (
    OPEN_INTEREST_TRACK,
    ObservedOpenInterest,
    canonical_open_interest,
    classify_open_interest,
    open_interest_issue,
)
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.market_data_source import OpenInterestSource
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class Clock(Protocol):
    def now_ms(self) -> int: ...


class ImportOpenInterestWindow:
    """Import one REST open interest window inside a single storage transaction.

    Writes only the open interest table, track progress and quarantine; candles
    and stream lifecycle are never touched.
    """

    def __init__(
        self,
        source: OpenInterestSource,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
    ) -> None:
        self._source = source
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock

    def execute(self, stream: StreamKey, window: TimeWindow) -> ImportWindowResult:
        points = self._source.fetch_open_interest(
            stream, window, observed_at_ms=self._clock.now_ms()
        )
        counts = {classification: 0 for classification in IngestionClassification}
        unexpected = 0
        latest: int | None = None
        with self._unit_of_work_factory() as unit_of_work:
            for point in points:
                if point.stream != stream or not window.contains(point.open_time_ms):
                    unexpected += 1
                    self._quarantine(
                        unit_of_work, stream, point, "unexpected_historical_open_interest"
                    )
                    continue
                issue = open_interest_issue(point)
                if issue is not None:
                    counts[IngestionClassification.REJECTED_INVALID] += 1
                    self._quarantine(unit_of_work, stream, point, issue)
                    continue
                incoming = canonical_open_interest(point, committed_at_ms=self._clock.now_ms())
                existing = unit_of_work.get_open_interest(stream, point.open_time_ms)
                classification = classify_open_interest(incoming, existing)
                if classification is not IngestionClassification.DUPLICATE:
                    unit_of_work.save_open_interest(incoming)
                counts[classification] += 1
                latest = point.open_time_ms if latest is None else max(latest, point.open_time_ms)
            if latest is not None:
                progress = unit_of_work.get_track_progress(stream, OPEN_INTEREST_TRACK)
                previous = progress.latest_committed_open_time_ms
                if previous is None or latest > previous:
                    unit_of_work.save_track_progress(
                        replace(progress, latest_committed_open_time_ms=latest)
                    )
            unit_of_work.commit()
        return ImportWindowResult(
            observed=len(points),
            committed=counts[IngestionClassification.COMMITTED],
            duplicates=counts[IngestionClassification.DUPLICATE],
            corrected=counts[IngestionClassification.CORRECTED],
            rejected=counts[IngestionClassification.REJECTED_INVALID],
            unexpected=unexpected,
        )

    def _quarantine(
        self,
        unit_of_work: CanonicalStorageUnitOfWork,
        stream: StreamKey,
        point: ObservedOpenInterest,
        reason_code: str,
    ) -> None:
        start_ms = max(0, point.open_time_ms)
        unit_of_work.record_quarantine(
            stream=stream,
            start_ms=start_ms,
            end_ms=start_ms + 1,
            reason_code=reason_code,
            detail=(
                f"observed_stream={point.stream.canonical_id} "
                f"open_interest={point.open_interest!r}"
            ),
            payload_json=None,
            created_at_ms=self._clock.now_ms(),
        )
