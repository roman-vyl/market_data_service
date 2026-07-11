"""Pure readiness projections from persisted per-stream state."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamStateSnapshot


@dataclass(frozen=True, slots=True)
class StreamReadiness:
    stream: StreamKey
    ready: bool
    state: str
    reason: str | None


def project_stream_readiness(snapshot: StreamStateSnapshot) -> StreamReadiness:
    """Project one durable lifecycle snapshot into the public readiness shape."""

    reason = None if snapshot.is_ready else snapshot.last_error_code or snapshot.state.value
    return StreamReadiness(
        stream=snapshot.stream,
        ready=snapshot.is_ready,
        state=snapshot.state.value,
        reason=reason,
    )


def strict_aggregate_readiness(snapshots: Iterable[StreamStateSnapshot]) -> bool:
    """Return true only when at least one stream exists and every stream is ready."""

    materialized = tuple(snapshots)
    return bool(materialized) and all(snapshot.is_ready for snapshot in materialized)
