"""Dispatch realtime connector, supervisor, and REST recovery components."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

from market_data_service.application.realtime.connector import RealtimeConnector
from market_data_service.application.realtime.events import (
    CandleObserved,
    RealtimeEvent,
    RecoveryReason,
    RecoveryRequired,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome
from market_data_service.application.realtime.recovery import RealtimeRecoveryCoordinator
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.status import RuntimeStatusStore


class RuntimeRealtimeCoordinator:
    def __init__(
        self,
        *,
        streams: Sequence[StreamKey],
        connector: RealtimeConnector,
        supervisor: RealtimeSupervisor,
        recovery: RealtimeRecoveryCoordinator,
        lifecycle: RuntimeLifecycleRecorder,
        status: RuntimeStatusStore,
        admission: RealtimeAdmissionGate,
        operation_gate: asyncio.Lock,
        now_ms: Callable[[], int],
        max_backfill_windows: int,
        max_repair_windows: int,
        stale_check_seconds: float = 1.0,
    ) -> None:
        self._streams = tuple(streams)
        self._connector = connector
        self._supervisor = supervisor
        self._recovery = recovery
        self._lifecycle = lifecycle
        self._status = status
        self._admission = admission
        self._operation_gate = operation_gate
        self._now_ms = now_ms
        self._max_backfill_windows = max_backfill_windows
        self._max_repair_windows = max_repair_windows
        self._stale_check_seconds = stale_check_seconds
        self._queue: asyncio.Queue[RecoveryRequired] = asyncio.Queue()
        self._pending: set[StreamKey] = set()

    async def run(self, stop_event: asyncio.Event) -> None:
        connector = asyncio.create_task(self._connector.run(stop_event))
        recovery = asyncio.create_task(self._recovery_worker(stop_event))
        stale = asyncio.create_task(self._stale_worker(stop_event))
        try:
            await connector
        finally:
            stop_event.set()
            await asyncio.gather(recovery, stale, return_exceptions=True)
            self._refresh_status()

    async def admit(self, stream: StreamKey) -> None:
        self._admission.admit(stream)
        self._status.clear_blocking_reason(stream)
        facts = self._supervisor.facts(stream)
        if facts.subscription_active and not facts.recovery_pending:
            await self._enqueue(
                RecoveryRequired(
                    stream=stream,
                    reason=RecoveryReason.STARTUP_RECONCILIATION,
                    detected_at_ms=self._now_ms(),
                )
            )
        self._sync_lifecycle()

    async def on_event(self, event: RealtimeEvent) -> None:
        if isinstance(event, CandleObserved) and not self._admission.allows(event.stream):
            return
        signals = tuple(
            signal
            for signal in self._supervisor.observe_event(event)
            if self._admission.allows(signal.stream)
        )
        if isinstance(event, SubscriptionConfirmed):
            for stream in self._streams:
                if not self._admission.allows(stream):
                    continue
                facts = self._supervisor.facts(stream)
                if facts.subscription_active and not facts.recovery_restored:
                    await self._enqueue(
                        RecoveryRequired(
                            stream=stream,
                            reason=RecoveryReason.STARTUP_RECONCILIATION,
                            detected_at_ms=self._now_ms(),
                        )
                    )
        for signal in signals:
            await self._enqueue(signal)
        self._sync_lifecycle()

    async def on_outcome(self, outcome: RealtimeIngestionOutcome) -> None:
        if not self._admission.allows(outcome.stream):
            return
        for signal in self._supervisor.observe_outcome(outcome):
            await self._enqueue(signal)
        self._sync_lifecycle()

    async def _enqueue(self, signal: RecoveryRequired) -> None:
        if signal.stream in self._pending:
            return
        self._pending.add(signal.stream)
        await self._queue.put(signal)

    async def _recovery_worker(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set() or not self._queue.empty():
            try:
                signal = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            try:
                async with self._operation_gate:
                    result = await self._recovery.execute(
                        RealtimeRecoveryRequest(
                            signal=signal,
                            max_backfill_windows=self._max_backfill_windows,
                            max_repair_windows=self._max_repair_windows,
                        )
                    )
                fatal = result.classification is RecoveryClassification.FATAL_FAILURE
                self._supervisor.record_recovery_result(
                    signal.stream,
                    restored=result.restored,
                    fatal=fatal,
                    restored_through_open_time_ms=result.restored_through_open_time_ms,
                )
            finally:
                self._pending.discard(signal.stream)
                self._queue.task_done()
                self._sync_lifecycle()

    async def _stale_worker(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            for signal in self._supervisor.detect_stale():
                if self._admission.allows(signal.stream):
                    await self._enqueue(signal)
            self._sync_lifecycle()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._stale_check_seconds)
            except TimeoutError:
                continue

    def _sync_lifecycle(self) -> None:
        for facts in self._supervisor.all_facts():
            durable = self._lifecycle.snapshot(facts.stream)
            if not self._admission.allows(facts.stream):
                self._status.update_stream(durable, facts)
                continue
            if facts.fatal_error_code and durable.state is not StreamLifecycleState.FAILED:
                durable = self._lifecycle.mark_failed(
                    facts.stream, reason=facts.fatal_error_code
                )
            elif facts.realtime_ready and durable.state is StreamLifecycleState.CONNECTING:
                durable = self._lifecycle.mark_ready(facts.stream)
            elif (
                facts.recovery_pending
                or facts.status.value in {"disconnected", "stale", "recovery_required"}
            ) and durable.state in {
                StreamLifecycleState.CONNECTING,
                StreamLifecycleState.READY,
            }:
                durable = self._lifecycle.mark_degraded(
                    facts.stream, reason=facts.status.value
                )
            self._status.update_stream(durable, facts)

    def _refresh_status(self) -> None:
        for facts in self._supervisor.all_facts():
            self._status.update_stream(self._lifecycle.snapshot(facts.stream), facts)
