"""Sequential fair ownership of incomplete historical reconciliation."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import (
    ReconciliationWindow,
    StartupClassification,
    StartupStreamOutcome,
)
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass(slots=True)
class _PendingStream:
    stream: StreamKey
    window: ReconciliationWindow | None
    failures: int = 0
    due_at: float = 0.0


class HistoricalReconciliationWorker:
    def __init__(
        self,
        *,
        coordinator: StartupCoordinator,
        initial_outcomes: Iterable[StartupStreamOutcome],
        status: RuntimeStatusStore,
        operation_gate: asyncio.Lock,
        on_complete: Callable[[StreamKey], Awaitable[None]],
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        idle_seconds: float = 0.1,
    ) -> None:
        self._coordinator = coordinator
        self._status = status
        self._operation_gate = operation_gate
        self._on_complete = on_complete
        self._base_backoff = base_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._idle_seconds = idle_seconds
        self._logger = logging.getLogger("market_data_service.runtime.historical")
        self._queue: deque[_PendingStream] = deque(
            _PendingStream(outcome.stream, outcome.window)
            for outcome in initial_outcomes
            if outcome.classification
            in {StartupClassification.INCOMPLETE, StartupClassification.RECOVERABLE_FAILURE}
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            if not self._queue:
                await self._wait(stop_event, self._idle_seconds)
                continue
            pending = self._queue.popleft()
            loop = asyncio.get_running_loop()
            now = loop.time()
            if pending.due_at > now:
                self._queue.append(pending)
                await self._wait(stop_event, min(self._idle_seconds, pending.due_at - now))
                continue
            self._status.set_blocking_reason(pending.stream, "historical_reconciliation")
            self._logger.info(
                "historical pass begin stream=%s", pending.stream.canonical_id
            )
            async with self._operation_gate:
                outcome = await asyncio.to_thread(
                    self._coordinator.execute_stream,
                    pending.stream,
                    pending.window,
                )
            self._logger.info(
                "historical pass end stream=%s classification=%s",
                pending.stream.canonical_id,
                outcome.classification.value,
            )
            if outcome.classification is StartupClassification.CONNECTING:
                self._status.clear_blocking_reason(pending.stream)
                await self._on_complete(pending.stream)
                continue
            if outcome.classification is StartupClassification.FATAL_FAILURE:
                self._status.set_blocking_reason(pending.stream, "historical_fatal_failure")
                continue
            pending.window = outcome.window or pending.window
            if outcome.classification is StartupClassification.RECOVERABLE_FAILURE:
                pending.failures += 1
                delay = min(
                    self._max_backoff,
                    self._base_backoff * (2 ** (pending.failures - 1)),
                )
                pending.due_at = loop.time() + delay
                self._status.set_blocking_reason(pending.stream, "historical_backoff")
                self._logger.warning(
                    "historical backoff stream=%s delay_seconds=%s error=%s",
                    pending.stream.canonical_id,
                    delay,
                    outcome.error_code,
                )
            else:
                pending.failures = 0
                pending.due_at = 0.0
                self._status.set_blocking_reason(
                    pending.stream, "historical_reconciliation"
                )
            self._queue.append(pending)

    @staticmethod
    async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(seconds, 0.0))
        except TimeoutError:
            return
