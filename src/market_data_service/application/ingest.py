"""Application use case for one canonical candle observation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

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
        with self._unit_of_work_factory() as unit_of_work:
            result = self.execute_in_unit_of_work(
                unit_of_work,
                candle,
                committed_at_ms=committed_at_ms,
            )
            unit_of_work.commit()
            return result

    @staticmethod
    def execute_in_unit_of_work(
        unit_of_work: CanonicalStorageUnitOfWork,
        candle: ObservedCandle,
        *,
        committed_at_ms: int,
    ) -> IngestionResult:
        """Apply the canonical ingestion decision inside an existing transaction."""

        issues = validate_observed_candle(candle)
        if not unit_of_work.stream_exists(candle.stream):
            return IngestionResult(IngestionClassification.REJECTED_UNCONFIGURED)

        if issues:
            classification = (
                IngestionClassification.REJECTED_UNCONFIRMED
                if any(issue.code.value == "unconfirmed" for issue in issues)
                else IngestionClassification.REJECTED_INVALID
            )
            issue_codes = tuple(issue.code.value for issue in issues)
            start_ms = max(0, candle.open_time_ms)
            end_ms = max(start_ms + 1, candle.close_time_ms + 1)
            unit_of_work.record_quarantine(
                stream=candle.stream,
                start_ms=start_ms,
                end_ms=end_ms,
                reason_code="candle_validation_failed",
                detail="; ".join(f"{issue.code.value}: {issue.detail}" for issue in issues),
                payload_json=None,
                created_at_ms=committed_at_ms,
            )
            return IngestionResult(classification, issue_codes)

        existing = unit_of_work.get_candle(candle.stream, candle.open_time_ms)
        classification = classify_against_existing(existing, candle)
        canonical = CanonicalCandle.from_observation(candle, committed_at_ms=committed_at_ms)

        if classification is IngestionClassification.COMMITTED:
            unit_of_work.insert_candle(canonical)
            IngestObservedCandle._advance_stream_state(unit_of_work, canonical)
        elif classification is IngestionClassification.CORRECTED:
            IngestObservedCandle._handle_correction(unit_of_work, existing, canonical)

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
