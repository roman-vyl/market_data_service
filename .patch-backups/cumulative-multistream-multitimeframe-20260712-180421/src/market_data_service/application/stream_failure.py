"""Persist classified stream failures for bootstrap and backfill use cases."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from market_data_service.application.backfill_errors import classify_backfill_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import InvalidStreamTransition, transition_stream_state
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


def record_stream_failure(
    unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
    stream: StreamKey,
    exc: Exception,
    *,
    now_ms: int,
) -> None:
    decision = classify_backfill_failure(exc)
    with unit_of_work_factory() as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
        try:
            failed_or_degraded = transition_stream_state(
                snapshot,
                decision.target_state,
                changed_at_ms=now_ms,
                error_code=decision.code,
                error_detail=decision.detail,
            )
        except InvalidStreamTransition:
            failed_or_degraded = replace(
                snapshot,
                last_error_code=decision.code,
                last_error_detail=decision.detail,
                updated_at_ms=now_ms,
            )
        unit_of_work.save_stream_state(failed_or_degraded)
        unit_of_work.commit()
