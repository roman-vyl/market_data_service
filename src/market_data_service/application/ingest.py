"""Application use case for one canonical candle observation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from market_data_service.domain.candle_comparison import classify_against_existing
from market_data_service.domain.candle_validation import validate_observed_candle
from market_data_service.domain.candles import CanonicalCandle, ObservationSource, ObservedCandle
from market_data_service.domain.classification import IngestionClassification
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


@dataclass(frozen=True, slots=True)
class IngestionResult:
    classification: IngestionClassification
    issue_codes: tuple[str, ...] = ()


class IngestObservedCandle:
    """Validate, classify, and atomically persist one observation."""

    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory

    def execute(self, candle: ObservedCandle, *, committed_at_ms: int) -> IngestionResult:
        issues = validate_observed_candle(candle)
        if issues:
            classification = (
                IngestionClassification.REJECTED_UNCONFIRMED
                if any(issue.code.value == "unconfirmed" for issue in issues)
                else IngestionClassification.REJECTED_INVALID
            )
            return IngestionResult(classification, tuple(issue.code.value for issue in issues))

        with self._unit_of_work_factory() as unit_of_work:
            if not unit_of_work.stream_exists(candle.stream):
                return IngestionResult(IngestionClassification.REJECTED_UNCONFIGURED)

            existing = unit_of_work.get_candle(candle.stream, candle.open_time_ms)
            classification = classify_against_existing(existing, candle)
            canonical = CanonicalCandle.from_observation(candle, committed_at_ms=committed_at_ms)

            if classification is IngestionClassification.COMMITTED:
                unit_of_work.insert_candle(canonical)
                self._advance_stream_state(unit_of_work, canonical)
            elif classification is IngestionClassification.CORRECTED:
                self._handle_correction(unit_of_work, existing, canonical)

            unit_of_work.commit()
            return IngestionResult(classification)

    @staticmethod
    def _advance_stream_state(
        unit_of_work: CanonicalStorageUnitOfWork,
        candle: CanonicalCandle,
    ) -> None:
        state = unit_of_work.get_stream_state(candle.stream)
        latest = state.latest_committed_open_time_ms
        if latest is None or candle.open_time_ms > latest:
            state = replace(
                state,
                latest_committed_open_time_ms=candle.open_time_ms,
                updated_at_ms=max(state.updated_at_ms, candle.committed_at_ms),
            )
            unit_of_work.save_stream_state(state)

    @staticmethod
    def _handle_correction(
        unit_of_work: CanonicalStorageUnitOfWork,
        existing: CanonicalCandle | None,
        incoming: CanonicalCandle,
    ) -> None:
        if existing is None:
            raise RuntimeError("correction requires an existing candle")
        detail = f"existing={existing.ohlcv_text}; incoming={incoming.ohlcv_text}"
        unit_of_work.record_quarantine(
            stream=incoming.stream,
            start_ms=incoming.open_time_ms,
            end_ms=incoming.close_time_ms + 1,
            reason_code="candle_correction_detected",
            detail=detail,
            payload_json=None,
            created_at_ms=incoming.committed_at_ms,
        )
        if incoming.source is ObservationSource.BYBIT_REST:
            unit_of_work.replace_candle(incoming)
            IngestObservedCandle._advance_stream_state(unit_of_work, incoming)
