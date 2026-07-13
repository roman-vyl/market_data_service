"""Fair runtime ownership of non-terminal realtime recovery."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.application.realtime.recovery import RealtimeRecoveryCoordinator
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass(slots=True)
class _PendingRecovery:
    signal: RecoveryRequired
    failures: int = 0
    due_at: float = 0.0


class RealtimeRecoveryWorker:
    def __init__(
        self,
        *,
        recovery: RealtimeRecoveryCoordinator,
        supervisor: RealtimeSupervisor,
        status: RuntimeStatusStore,
        operation_gate: asyncio.Lock,
        sync_lifecycle: Callable[[], None],
        max_backfill_windows: int,
        max_repair_windows: int,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        idle_seconds: float = 0.1,
    ) -> None:
        self._recovery = recovery
        self._supervisor = supervisor
        self._status = status
        self._operation_gate = operation_gate
        self._sync_lifecycle = sync_lifecycle
        self._max_backfill_windows = max_backfill_windows
        self._max_repair_windows = max_repair_windows
        self._base_backoff = base_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._idle_seconds = idle_seconds
        self._queue: asyncio.Queue[_PendingRecovery] = asyncio.Queue()
        self._pending: set[StreamKey] = set()
        self._logger = logging.getLogger("market_data_service.runtime.realtime")

    async def enqueue(self, signal: RecoveryRequired) -> None:
        if signal.stream in self._pending:
            return
        self._pending.add(signal.stream)
        await self._queue.put(_PendingRecovery(signal))

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set() or not self._queue.empty():
            try:
                pending = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            loop = asyncio.get_running_loop()
            now = loop.time()
            if pending.due_at > now:
                await self._queue.put(pending)
                self._queue.task_done()
                await self._wait(stop_event, min(self._idle_seconds, pending.due_at - now))
                continue
            retry = await self._execute(pending, loop)
            if retry is not None and not stop_event.is_set():
                await self._queue.put(retry)
            else:
                self._pending.discard(pending.signal.stream)
            self._queue.task_done()
            self._sync_lifecycle()

    async def _execute(
        self,
        pending: _PendingRecovery,
        loop: asyncio.AbstractEventLoop,
    ) -> _PendingRecovery | None:
        async with self._operation_gate:
            result = await self._recovery.execute(
                RealtimeRecoveryRequest(
                    signal=pending.signal,
                    max_backfill_windows=self._max_backfill_windows,
                    max_repair_windows=self._max_repair_windows,
                )
            )
        fatal = result.classification is RecoveryClassification.FATAL_FAILURE
        self._supervisor.record_recovery_result(
            pending.signal.stream,
            restored=result.restored,
            fatal=fatal,
            restored_through_open_time_ms=result.restored_through_open_time_ms,
        )
        if result.classification is RecoveryClassification.RESTORED:
            self._status.clear_blocking_reason(pending.signal.stream)
            return None
        if result.classification is RecoveryClassification.INCOMPLETE:
            pending.failures = 0
            pending.due_at = 0.0
            self._status.set_blocking_reason(pending.signal.stream, "realtime_recovery")
            self._logger.info(
                "realtime recovery incomplete stream=%s",
                pending.signal.stream.canonical_id,
            )
            return pending
        if result.classification is RecoveryClassification.RECOVERABLE_FAILURE:
            pending.failures += 1
            delay = min(self._max_backoff, self._base_backoff * (2 ** (pending.failures - 1)))
            pending.due_at = loop.time() + delay
            self._status.set_blocking_reason(
                pending.signal.stream, "realtime_recovery_backoff"
            )
            self._logger.warning(
                "realtime recovery backoff stream=%s delay_seconds=%s error=%s",
                pending.signal.stream.canonical_id,
                delay,
                result.error_code,
            )
            return pending
        return None

    @staticmethod
    async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(seconds, 0.0))
        except TimeoutError:
            return
