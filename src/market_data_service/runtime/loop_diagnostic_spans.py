"""Bounded synchronous timing samples and overlap accounting."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from market_data_service.ports.operation_timing import OperationSpan

_NS_PER_MS = 1_000_000
_SLOW_NS = 50 * _NS_PER_MS


def milliseconds(ns: int) -> float:
    return ns / _NS_PER_MS


@dataclass(slots=True)
class PhaseStats:
    count: int = 0
    total_ns: int = 0
    max_ns: int = 0
    slow: int = 0

    def add(self, duration_ns: int) -> None:
        self.count += 1
        self.total_ns += duration_ns
        self.max_ns = max(self.max_ns, duration_ns)
        self.slow += duration_ns >= _SLOW_NS

    def fields(self) -> dict[str, int | float]:
        return {
            "count": self.count,
            "total_ms": milliseconds(self.total_ns),
            "max_ms": milliseconds(self.max_ns),
            "slow": self.slow,
        }


@dataclass(frozen=True, slots=True)
class TimingSample:
    span: OperationSpan
    reads: dict[str, int | float] | None = None

    def fields(self, overlap_ns: int) -> dict[str, object]:
        s = self.span
        fields: dict[str, object] = {
            "operation": s.operation,
            "phase": s.phase,
            "stream": None if s.stream is None else s.stream.canonical_id,
            "duration_ms": milliseconds(s.duration_ns),
            "overlap_ms": milliseconds(overlap_ns),
        }
        if self.reads is not None:
            fields["reads"] = self.reads
        return fields


class LoopDiagnosticSpans:
    def __init__(self, capacity: int = 128) -> None:
        self._recent: deque[TimingSample] = deque(maxlen=capacity)
        self._stats: dict[tuple[str, str], PhaseStats] = {}
        self._reads = PhaseStats()
        self._evicted = 0
        self._evicted_since_tick = 0

    def record(self, span: OperationSpan) -> None:
        key = (span.operation, span.phase)
        stats = self._stats.get(key)
        if stats is None:
            self._stats[key] = stats = PhaseStats()
        stats.add(span.duration_ns)
        reads = None
        if span.operation == "lifecycle.sync":
            if span.phase == "snapshot":
                self._reads.add(span.duration_ns)
            elif span.phase == "total":
                reads = self._reads.fields()
                self._reads = PhaseStats()
        # Keep every parent interval so consecutive short calls contribute to coverage.
        if span.phase == "total" or span.duration_ns >= _SLOW_NS:
            if len(self._recent) == self._recent.maxlen:
                self._evicted += 1
                self._evicted_since_tick += 1
            self._recent.append(TimingSample(span, reads))

    def overlap(self, start_ns: int, end_ns: int) -> tuple[int, list[dict[str, object]], int]:
        intersections = []
        for sample in self._recent:
            span = sample.span
            start, end = max(start_ns, span.start_ns), min(end_ns, span.end_ns)
            if start < end:
                intersections.append((start, end, sample))
        intersections.sort(key=lambda x: x[0])
        cursor = start_ns
        covered = 0
        for start, end, _ in intersections:
            covered += max(0, end - max(cursor, start))
            cursor = max(cursor, end)
        intersections.sort(key=lambda x: x[1] - x[0], reverse=True)
        return (
            covered,
            [sample.fields(end - start) for start, end, sample in intersections[:3]],
            self._evicted_since_tick,
        )

    def finish_tick(self) -> None:
        self._recent.clear()
        self._evicted_since_tick = 0

    def take_aggregate(self) -> tuple[list[dict[str, object]], int]:
        phases: list[dict[str, object]] = [
            {"operation": operation, "phase": phase, **stats.fields()}
            for (operation, phase), stats in sorted(self._stats.items())
        ]
        evicted = self._evicted
        self._stats.clear()
        self._evicted = 0
        return phases, evicted
