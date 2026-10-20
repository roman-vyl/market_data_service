"""Fair runtime ownership of non-terminal realtime recovery."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

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
from market_data_service.runtime.realtime_recovery_diagnostics import (
    RealtimeRecoveryDiagnostics,
)
from market_data_service.runtime.realtime_recovery_intent import RecoveryIntent
from market_data_service.runtime.status import RuntimeStatusStore


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
        stale_first_retry_seconds: float = 60.0,
        stale_next_retry_seconds: float = 360.0,
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
        self._stale_first_retry = stale_first_retry_seconds
        self._stale_next_retry = stale_next_retry_seconds
        self._idle_seconds = idle_seconds
        self._queue: asyncio.Queue[RecoveryIntent] = asyncio.Queue()
        self._intents: dict[StreamKey, RecoveryIntent] = {}
        self._logger = logging.getLogger("market_data_service.runtime.realtime")
        self._diagnostics = RealtimeRecoveryDiagnostics(self._logger)

    async def enqueue(self, signal: RecoveryRequired) -> None:
        """Merge the signal into the stream's single intent; never start parallel work."""

        intent = self._intents.get(signal.stream)
        if intent is not None:
            intent.absorb(signal)
            return
        intent = RecoveryIntent.from_signal(signal)
        self._intents[signal.stream] = intent
        await self._queue.put(intent)

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                try:
                    intent = await asyncio.wait_for(self._queue.get(), timeout=0.2)
                except TimeoutError:
                    continue
                loop = asyncio.get_running_loop()
                now = loop.time()
                if intent.due_at > now:
                    await self._queue.put(intent)
                    self._queue.task_done()
                    await self._wait(stop_event, min(self._idle_seconds, intent.due_at - now))
                    continue
                retry = await self._execute(intent, loop)
                if retry is not None and not stop_event.is_set():
                    await self._queue.put(retry)
                else:
                    self._intents.pop(intent.stream, None)
                self._queue.task_done()
                self._sync_lifecycle()
        finally:
            self._intents.clear()

    async def _execute(
        self,
        intent: RecoveryIntent,
        loop: asyncio.AbstractEventLoop,
    ) -> RecoveryIntent | None:
        started_revision = intent.revision
        async with self._operation_gate:
            result = await self._recovery.execute(
                RealtimeRecoveryRequest(
                    signal=intent.request_signal(),
                    max_windows=self._max_windows,
                    recovery_window=intent.recovery_window,
                )
            )
        # A requirement that changed during the attempt makes its outcome incomplete.
        changed = intent.revision != started_revision
        previous_marker = intent.progress_marker
        fatal = result.classification is RecoveryClassification.FATAL_FAILURE
        self._supervisor.record_recovery_result(
            intent.stream,
            restored=result.restored and not changed,
            fatal=fatal,
            restored_through_open_time_ms=result.restored_through_open_time_ms,
        )
        if fatal:
            self._log_result(intent, result, previous_marker, delay=0.0)
            return None
        if changed:
            self._status.set_blocking_reason(intent.stream, "realtime_recovery")
            self._log_result(intent, result, previous_marker, delay=0.0)
            return intent
        if result.classification is RecoveryClassification.RESTORED:
            self._status.clear_blocking_reason(intent.stream)
            self._log_result(intent, result, previous_marker, delay=0.0)
            return None
        delay = 0.0
        reason = "realtime_recovery"
        if result.classification is RecoveryClassification.INCOMPLETE:
            previous_window = intent.recovery_window
            intent.recovery_window = result.next_recovery_window or result.recovery_window
            intent.progress_marker = result.progress_marker
            progressed = result.made_progress or (
                result.next_recovery_window is not None
                and result.next_recovery_window != previous_window
            )
            if not progressed and previous_marker is not None:
                progressed = result.progress_marker != previous_marker
            if progressed:
                intent.attempts_without_progress = 0
            else:
                delay = self._retry_delay(intent)
                reason = "realtime_recovery_no_progress_backoff"
        else:
            delay = self._retry_delay(intent)
            reason = "realtime_recovery_backoff"
            self._logger.warning(
                "realtime recovery backoff stream=%s delay_seconds=%s error=%s",
                intent.stream.canonical_id,
                delay,
                result.error_code,
            )
        intent.due_at = loop.time() + delay if delay else 0.0
        self._status.set_blocking_reason(intent.stream, reason)
        self._log_result(intent, result, previous_marker, delay=delay)
        return intent

    def _retry_delay(self, intent: RecoveryIntent) -> float:
        """Urgent: fast doubling backoff. Background (stale): fixed 60 s, then 360 s."""

        intent.attempts_without_progress += 1
        attempts = intent.attempts_without_progress
        if intent.urgent:
            return min(self._max_backoff, self._base_backoff * 2.0 ** (attempts - 1))
        return self._stale_first_retry if attempts == 1 else self._stale_next_retry

    def _log_result(
        self,
        intent: RecoveryIntent,
        result: RealtimeRecoveryResult,
        previous_marker: RepairProgressMarker | None,
        *,
        delay: float,
    ) -> None:
        self._diagnostics.record(
            stream=intent.stream,
            reason=intent.reason.value,
            before=previous_marker,
            result=result,
            delay_seconds=delay,
            no_progress_attempts=intent.attempts_without_progress,
        )

    @staticmethod
    async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(seconds, 0.0))
        except TimeoutError:
            return
