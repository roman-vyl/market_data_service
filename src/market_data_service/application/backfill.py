"""Application boundary for finite sequential history loading."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from market_data_service.domain.backfill import BackfillRequest
from market_data_service.domain.identity import StreamKey


@dataclass(frozen=True, slots=True)
class BackfillRunPlan:
    """Ordered streams and finite work budget for one command run."""

    streams: tuple[StreamKey, ...]
    max_windows_per_stream: int

    def __post_init__(self) -> None:
        if not self.streams:
            raise ValueError("backfill plan requires at least one stream")
        if len(set(self.streams)) != len(self.streams):
            raise ValueError("backfill plan streams must be unique")
        if self.max_windows_per_stream <= 0:
            raise ValueError("max_windows_per_stream must be positive")


def plan_sequential_backfill(
    request: BackfillRequest,
    configured_streams: Sequence[StreamKey],
) -> BackfillRunPlan:
    """Resolve one deterministic sequential run without performing I/O."""

    unique_streams = tuple(dict.fromkeys(configured_streams))
    if request.stream is not None:
        if request.stream not in unique_streams:
            raise ValueError(f"stream is not configured: {request.stream}")
        selected = (request.stream,)
    else:
        if not unique_streams:
            raise ValueError("no configured streams available for backfill")
        selected = unique_streams

    return BackfillRunPlan(
        streams=selected,
        max_windows_per_stream=request.budget.max_windows,
    )
