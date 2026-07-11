"""Half-open time-window contracts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, order=True)
class TimeWindow:
    """Immutable half-open interval ``[start_ms, end_ms)``."""

    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if self.start_ms >= self.end_ms:
            raise ValueError("time window must satisfy start_ms < end_ms")

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms < self.end_ms
