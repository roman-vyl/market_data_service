"""Scoped, transport-neutral timing of synchronous runtime work."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter_ns

from market_data_service.domain.identity import StreamKey


@dataclass(frozen=True, slots=True)
class OperationSpan:
    operation: str
    phase: str
    stream: StreamKey | None
    start_ns: int
    end_ns: int

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns


SpanSink = Callable[[OperationSpan], None]
_sink: ContextVar[SpanSink | None] = ContextVar("operation_span_sink", default=None)
_scope: ContextVar[tuple[str, StreamKey | None] | None] = ContextVar(
    "operation_scope", default=None
)


@contextmanager
def observe_operations(sink: SpanSink) -> Iterator[None]:
    token = _sink.set(sink)
    try:
        yield
    finally:
        _sink.reset(token)


@contextmanager
def measure_operation(operation: str, stream: StreamKey | None = None) -> Iterator[None]:
    sink = _sink.get()
    if sink is None:
        yield
        return
    # A scope covers only a synchronous call, never an await.
    token = _scope.set((operation, stream))
    start = perf_counter_ns()
    try:
        yield
    finally:
        end = perf_counter_ns()
        _scope.reset(token)
        sink(OperationSpan(operation, "total", stream, start, end))


@contextmanager
def measure_phase(phase: str, stream: StreamKey | None = None) -> Iterator[None]:
    scope = _scope.get()
    if scope is None:
        yield
        return
    sink = _sink.get()
    assert sink is not None
    operation, parent_stream = scope
    start = perf_counter_ns()
    try:
        yield
    finally:
        sink(OperationSpan(operation, phase, stream or parent_stream, start, perf_counter_ns()))
