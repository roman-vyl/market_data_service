"""Long-running market-data service process composition."""

from __future__ import annotations

import asyncio
import logging

from market_data_service.adapters.bybit.websocket import (
    BybitTopicMap,
    BybitWebSocketAdapter,
    WebsocketsTransport,
)
from market_data_service.adapters.http import RuntimeHttpServer
from market_data_service.application.realtime.connector import (
    RealtimeConnector,
    ReconnectPolicy,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.application.realtime.supervisor_types import StalePolicy
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.admission import (
    AdmissionGatedCandleHandler,
    RealtimeAdmissionGate,
)
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import (
    StartupClassification,
    StartupStreamOutcome,
)
from market_data_service.runtime.status import RuntimeStatusStore
from market_data_service.runtime.wiring import RuntimeWiring


class RuntimeService:
    def __init__(
        self,
        *,
        settings: RuntimeSettings,
        config: ValidatedMarketConfig,
        wiring: RuntimeWiring,
        status: RuntimeStatusStore,
        http_server: RuntimeHttpServer,
    ) -> None:
        self._settings = settings
        self._config = config
        self._wiring = wiring
        self._status = status
        self._http_server = http_server
        self._logger = logging.getLogger("market_data_service.runtime")

    async def run(self, stop_event: asyncio.Event) -> None:
        self._http_server.start()
        try:
            coordinator, outcomes = self._startup()
            admitted = tuple(
                outcome.stream
                for outcome in outcomes
                if outcome.classification is StartupClassification.CONNECTING
            )
            operation_gate = asyncio.Lock()
            realtime = self._build_realtime(admitted, operation_gate)
            worker = HistoricalReconciliationWorker(
                coordinator=coordinator,
                initial_outcomes=outcomes,
                status=self._status,
                operation_gate=operation_gate,
                on_complete=realtime.admit,
                base_backoff_seconds=self._settings.historical_retry_base_seconds,
                max_backoff_seconds=self._settings.historical_retry_max_seconds,
            )
            self._status.mark_healthy()
            await asyncio.gather(
                realtime.run(stop_event),
                worker.run(stop_event),
            )
        except Exception as exc:
            self._status.mark_fatal(f"{type(exc).__name__}: {exc}")
            self._logger.exception("runtime failed")
            raise
        finally:
            self._http_server.close()

    def _startup(self) -> tuple[StartupCoordinator, tuple[StartupStreamOutcome, ...]]:
        reconciler = HistoricalStreamReconciler(
            lower_bound=self._wiring.lower_bound(),
            repair=self._wiring.repair(),
            lifecycle=self._wiring.lifecycle(),
            now_ms=self._wiring.clock.now_ms,
            discovery_windows_per_pass=(
                self._settings.startup_backfill_windows_per_stream
            ),
            repair_windows_per_pass=self._settings.startup_repair_windows_per_stream,
        )
        coordinator = StartupCoordinator(reconciler)
        outcomes = coordinator.execute(self._config.enabled_streams)
        lifecycle = self._wiring.lifecycle()
        for outcome in outcomes:
            if outcome.classification in {
                StartupClassification.INCOMPLETE,
                StartupClassification.RECOVERABLE_FAILURE,
            }:
                self._status.set_blocking_reason(
                    outcome.stream,
                    "historical_reconciliation"
                    if outcome.classification is StartupClassification.INCOMPLETE
                    else "historical_backoff",
                )
            elif outcome.classification is StartupClassification.FATAL_FAILURE:
                self._status.set_blocking_reason(
                    outcome.stream, "historical_fatal_failure"
                )
            self._status.update_stream(lifecycle.snapshot(outcome.stream), None)
            self._logger.info(
                "startup stream=%s classification=%s",
                outcome.stream.canonical_id,
                outcome.classification.value,
            )
        return coordinator, outcomes

    def _build_realtime(
        self,
        admitted_streams: tuple[StreamKey, ...],
        operation_gate: asyncio.Lock,
    ) -> RuntimeRealtimeCoordinator:
        topic_map = BybitTopicMap.from_config(self._config)
        streams = self._config.enabled_streams
        lifecycle = self._wiring.lifecycle()
        initial = {
            stream: lifecycle.snapshot(stream).latest_committed_open_time_ms
            for stream in streams
        }
        supervisor = RealtimeSupervisor(
            streams,
            topic_map.topic_to_stream,
            self._wiring.clock.now_ms,
            stale_policy=StalePolicy(
                intervals=self._settings.stale_intervals,
                grace_ms=self._settings.stale_grace_ms,
            ),
            initial_latest_open_time_ms=initial,
        )
        admission = RealtimeAdmissionGate(admitted_streams)
        holder: dict[str, RuntimeRealtimeCoordinator] = {}

        async def on_event(event: object) -> None:
            await holder["runtime"].on_event(event)  # type: ignore[arg-type]

        async def on_outcome(outcome: object) -> None:
            await holder["runtime"].on_outcome(outcome)  # type: ignore[arg-type]

        connector = RealtimeConnector(
            url=self._settings.websocket_url,
            transport=WebsocketsTransport(),
            adapter=BybitWebSocketAdapter(topic_map),
            candle_handler=AdmissionGatedCandleHandler(
                admission,
                self._wiring.candle_handler(),
            ),
            now_ms=self._wiring.clock.now_ms,
            on_event=on_event,
            on_outcome=on_outcome,
            reconnect_policy=ReconnectPolicy(
                max_attempts=self._settings.reconnect_max_attempts,
                delay_seconds=self._settings.reconnect_delay_seconds,
            ),
        )
        runtime = RuntimeRealtimeCoordinator(
            streams=streams,
            connector=connector,
            supervisor=supervisor,
            recovery=self._wiring.recovery(),
            lifecycle=lifecycle,
            status=self._status,
            admission=admission,
            operation_gate=operation_gate,
            now_ms=self._wiring.clock.now_ms,
            max_backfill_windows=self._settings.startup_backfill_windows_per_stream,
            max_repair_windows=self._settings.startup_repair_windows_per_stream,
        )
        holder["runtime"] = runtime
        return runtime
