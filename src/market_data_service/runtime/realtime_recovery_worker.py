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
    RealtimeRecoveryResult,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.application.repair_types import RepairProgressMarker
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow
from market_data_service.runtime.realtime_recovery_diagnostics import (
    RealtimeRecoveryDiagnostics,
)
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass(slots=True)
class _PendingRecovery:
    signal: RecoveryRequired
    failures: int = 0
    no_progress_attempts: int = 0
    due_at: float = 0.0
    recovery_window: TimeWindow | None = None
    progress_marker: RepairProgressMarker | None = None


class RealtimeRecoveryWorker:
    def __init__(
        self,
        *,
        recovery: RealtimeRecoveryCoordinator,
        supervisor: RealtimeSupervisor,
        status: RuntimeStatusStore,
        operation_gate: asyncio.Lock,
        sync_lifecycle: Callable[[], None],
        max_windows: int,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        idle_seconds: float = 0.1,
    ) -> None:
        self._recovery = recovery
        self._supervisor = supervisor
        self._status = status
        self._operation_gate = operation_gate
        self._sync_lifecycle = sync_lifecycle
        self._max_windows = max_windows
        self._base_backoff = base_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._idle_seconds = idle_seconds
        self._queue: asyncio.Queue[_PendingRecovery] = asyncio.Queue()
        self._pending: set[StreamKey] = set()
        self._logger = logging.getLogger("market_data_service.runtime.realtime")
        self._diagnostics = RealtimeRecoveryDiagnostics(self._logger)

    async def enqueue(self, signal: RecoveryRequired) -> None:
        if signal.stream in self._pending:
            return
        self._pending.add(signal.stream)
        await self._queue.put(_PendingRecovery(signal))

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                try:
                    pending = await asyncio.wait_for(self._queue.get(), timeout=0.2)
                except TimeoutError:
                    continue
                loop = asyncio.get_running_loop()
                now = loop.time()
                if pending.due_at > now:
                    await self._queue.put(pending)
                    self._queue.task_done()
                    await self._wait(
                        stop_event,
                        min(self._idle_seconds, pending.due_at - now),
                    )
                    continue
                retry = await self._execute(pending, loop)
                if retry is not None and not stop_event.is_set():
                    await self._queue.put(retry)
                else:
                    self._pending.discard(pending.signal.stream)
                self._queue.task_done()
                self._sync_lifecycle()
        finally:
            self._pending.clear()

    async def _execute(
        self,
        pending: _PendingRecovery,
        loop: asyncio.AbstractEventLoop,
    ) -> _PendingRecovery | None:
        async with self._operation_gate:
            result = await self._recovery.execute(
                RealtimeRecoveryRequest(
                    signal=pending.signal,
                    max_windows=self._max_windows,
                    recovery_window=pending.recovery_window,
                )
            )
        previous_marker = pending.progress_marker
        fatal = result.classification is RecoveryClassification.FATAL_FAILURE
        self._supervisor.record_recovery_result(
            pending.signal.stream,
            restored=result.restored,
            fatal=fatal,
            restored_through_open_time_ms=result.restored_through_open_time_ms,
        )
        if result.classification is RecoveryClassification.RESTORED:
            self._status.clear_blocking_reason(pending.signal.stream)
            self._log_result(pending, result, previous_marker, delay=0.0)
            return None
        if result.classification is RecoveryClassification.INCOMPLETE:
            pending.failures = 0
            previous_window = pending.recovery_window
            pending.recovery_window = result.next_recovery_window or result.recovery_window
            pending.progress_marker = result.progress_marker
            progressed = result.made_progress or (
                result.next_recovery_window is not None
                and result.next_recovery_window != previous_window
            )
            if not progressed and previous_marker is not None:
                progressed = result.progress_marker != previous_marker
            if progressed:
                pending.no_progress_attempts = 0
                pending.due_at = 0.0
                reason = "realtime_recovery"
                delay = 0.0
            else:
                pending.no_progress_attempts += 1
                delay = min(
                    self._max_backoff,
                    self._base_backoff * (2 ** (pending.no_progress_attempts - 1)),
                )
                pending.due_at = loop.time() + delay
                reason = "realtime_recovery_no_progress_backoff"
            self._status.set_blocking_reason(pending.signal.stream, reason)
            self._log_result(pending, result, previous_marker, delay=delay)
            return pending
        if result.classification is RecoveryClassification.RECOVERABLE_FAILURE:
            pending.failures += 1
            delay = min(self._max_backoff, self._base_backoff * (2 ** (pending.failures - 1)))
            pending.due_at = loop.time() + delay
            self._status.set_blocking_reason(pending.signal.stream, "realtime_recovery_backoff")
            self._log_result(pending, result, previous_marker, delay=delay)
            self._logger.warning(
                "realtime recovery backoff stream=%s delay_seconds=%s error=%s",
                pending.signal.stream.canonical_id,
                delay,
                result.error_code,
            )
            return pending
        self._log_result(pending, result, previous_marker, delay=0.0)
        return None

    def _log_result(
        self,
        pending: _PendingRecovery,
        result: RealtimeRecoveryResult,
        previous_marker: RepairProgressMarker | None,
        *,
        delay: float,
    ) -> None:
        self._diagnostics.record(
            stream=pending.signal.stream,
            reason=pending.signal.reason.value,
            before=previous_marker,
            result=result,
            delay_seconds=delay,
            no_progress_attempts=pending.no_progress_attempts,
        )

    @staticmethod
    async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(seconds, 0.0))
        except TimeoutError:
            return
