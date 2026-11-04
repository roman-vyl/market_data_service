"""Collect event loop diagnostics and hand bounded output to a worker."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from time import perf_counter_ns

from market_data_service.ports.operation_timing import observe_operations
from market_data_service.runtime.loop_diagnostic_spans import LoopDiagnosticSpans, milliseconds

_HOUR_NS = 3_600_000_000_000


class LoopDiagnostics:
    def __init__(
        self, logger: logging.Logger, *, capacity: int = 128, now_ns: int | None = None
    ) -> None:
        self.spans = LoopDiagnosticSpans(capacity)
        self._logger = logger
        self._output: asyncio.Queue[tuple[str, dict[str, object]]] = asyncio.Queue(capacity)
        self._dropped = 0
        self._coverage = dict(full=0, partial=0, zero=0)
        self._period_start = perf_counter_ns() if now_ns is None else now_ns

    def tick(self, deadline_ns: int, end_ns: int, *, warn: bool) -> None:
        if warn:
            lag_ns = max(0, end_ns - deadline_ns)
            covered, operations, evicted = self.spans.overlap(deadline_ns, end_ns)
            kind = "zero" if not covered else "full" if covered == lag_ns else "partial"
            self._coverage[kind] += 1
            self._offer(
                "event loop lag",
                {
                    "lag_ms": milliseconds(lag_ns),
                    "covered_ms": milliseconds(covered),
                    "unexplained_ms": milliseconds(lag_ns - covered),
                    "operations": operations,
                    "lost_measurements": evicted,
                    "incomplete": bool(evicted),
                },
            )
        self.spans.finish_tick()
        if end_ns - self._period_start >= _HOUR_NS:
            phases, evicted = self.spans.take_aggregate()
            self._offer(
                "event loop diagnostics",
                {
                    "period_ms": milliseconds(end_ns - self._period_start),
                    "phases": phases,
                    "coverage": self._coverage,
                    "lost_measurements": evicted,
                },
            )
            self._coverage = dict(full=0, partial=0, zero=0)
            self._period_start = end_ns

    def _offer(self, name: str, fields: dict[str, object]) -> None:
        fields["lost_output_events"] = self._dropped
        try:
            self._output.put_nowait((name, fields))
        except asyncio.QueueFull:
            self._dropped += 1

    async def emit(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                name, fields = await asyncio.wait_for(self._output.get(), timeout=0.5)
            except TimeoutError:
                continue
            try:
                await asyncio.to_thread(self._log, name, fields)
            except Exception:
                self._dropped += 1

    def _log(self, name: str, fields: dict[str, object]) -> None:
        payload = json.dumps(fields, separators=(",", ":"))
        if name == "event loop lag":
            lag_ms = fields["lag_ms"]
            assert isinstance(lag_ms, (int, float))
            self._logger.warning(
                "event loop lag %.2fs (loop was blocked) %s", lag_ms / 1000, payload
            )
        else:
            self._logger.info("%s %s", name, payload)


active_loop_diagnostics: ContextVar[LoopDiagnostics | None] = ContextVar(
    "active_loop_diagnostics", default=None
)


@asynccontextmanager
async def diagnostic_tasks(
    stop_event: asyncio.Event, logger: logging.Logger, *, diagnostics_enabled: bool
) -> AsyncIterator[asyncio.TaskGroup]:
    if not diagnostics_enabled:
        async with asyncio.TaskGroup() as group:
            yield group
        return
    diagnostics = LoopDiagnostics(logger)
    token = active_loop_diagnostics.set(diagnostics)
    try:
        with observe_operations(diagnostics.spans.record):
            async with asyncio.TaskGroup() as group:
                group.create_task(diagnostics.emit(stop_event))
                yield group
    finally:
        active_loop_diagnostics.reset(token)
