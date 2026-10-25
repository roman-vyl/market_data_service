"""Continuity frontier arithmetic shared by both data tracks of a stream."""

from __future__ import annotations

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe


def extend_frontier(previous: int | None, open_time_ms: int, stream: StreamKey) -> int:
    """A live point extends proven continuity only when it is the next bar."""

    step_ms = get_timeframe(stream.timeframe).duration_ms
    if previous is not None and open_time_ms > previous + step_ms:
        return previous
    return max(previous or open_time_ms, open_time_ms)


def advance_frontier(current: int | None, restored_through: int | None) -> int | None:
    return restored_through if current is None else max(current, restored_through or current)
