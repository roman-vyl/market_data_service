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
from market_data_service.runtime.event_loop_lag import EventLoopLagMonitor
from market_data_service.runtime.exception_groups import root_exception
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.open_interest_composition import (
    admit_with_open_interest,
    initial_open_interest_frontiers,
    open_interest_notification_worker,
    open_interest_startup,
    open_interest_workers,
)
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_diagnostics import StartupDiagnostics
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
        self._startup_diagnostics = StartupDiagnostics(self._logger)

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
                on_complete=admit_with_open_interest(self._wiring, self._config, realtime),
                base_backoff_seconds=self._settings.historical_retry_base_seconds,
                max_backoff_seconds=self._settings.historical_retry_max_seconds,
            )
            self._status.mark_healthy()
            async with asyncio.TaskGroup() as task_group:
                task_group.create_task(realtime.run(stop_event))
                task_group.create_task(worker.run(stop_event))
                task_group.create_task(
                    EventLoopLagMonitor(
                        self._logger, warn_seconds=self._settings.event_loop_lag_warn_seconds
                    ).run(stop_event)
                )
        except ExceptionGroup as exc:
            root = root_exception(exc)
            self._mark_fatal(root)
            raise root from exc
        except Exception as exc:
            self._mark_fatal(exc)
            raise
        finally:
            stop_event.set()
            self._http_server.close()

    def _startup(self) -> tuple[StartupCoordinator, tuple[StartupStreamOutcome, ...]]:
        reconciler = HistoricalStreamReconciler(
            lower_bound=self._wiring.lower_bound(),
            repair=self._wiring.repair(),
            lifecycle=self._wiring.lifecycle(),
            now_ms=self._wiring.clock.now_ms,
            discovery_windows_per_pass=(self._settings.startup_backfill_windows_per_stream),
            repair_windows_per_pass=self._settings.startup_repair_windows_per_stream,
            **open_interest_startup(self._wiring, self._settings),
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
                self._status.set_blocking_reason(outcome.stream, "historical_fatal_failure")
            self._status.update_stream(lifecycle.snapshot(outcome.stream), None)
            self._startup_diagnostics.record(outcome)
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
            stream: lifecycle.snapshot(stream).latest_committed_open_time_ms for stream in streams
        }
        oi_initial = initial_open_interest_frontiers(self._wiring, admitted_streams)
        supervisor = RealtimeSupervisor(
            streams,
            topic_map.topic_to_stream,
            self._wiring.clock.now_ms,
            stale_policy=StalePolicy(
                grace_ms=self._settings.stale_grace_ms,
            ),
            initial_latest_open_time_ms=initial,
            initial_open_interest_open_time_ms=oi_initial,
        )
        admission = RealtimeAdmissionGate(admitted_streams)
        holder: dict[str, RuntimeRealtimeCoordinator] = {}
        notifier_worker = open_interest_notification_worker(
            self._wiring,
            self._settings,
            supervisor,
            lambda s: holder["runtime"].request_recovery(s),
        )

        async def on_event(event: object) -> None:
            await holder["runtime"].on_event(event)  # type: ignore[arg-type]

        async def on_outcome(outcome: object) -> None:
            await holder["runtime"].on_outcome(outcome)  # type: ignore[arg-type]

        connector = RealtimeConnector(
            url=self._settings.websocket_url,
            transport=WebsocketsTransport(
                ping_timeout_seconds=self._settings.websocket_ping_timeout_seconds
            ),
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
            max_recovery_windows=self._settings.realtime_recovery_windows_per_stream,
            stale_check_seconds=self._settings.realtime_stale_check_seconds,
            recovery_base_backoff_seconds=(self._settings.realtime_recovery_base_seconds),
            recovery_max_backoff_seconds=(self._settings.realtime_recovery_max_seconds),
            stale_first_retry_seconds=self._settings.stale_recovery_first_retry_seconds,
            stale_next_retry_seconds=self._settings.stale_recovery_next_retry_seconds,
            recovery_idle_seconds=self._settings.realtime_recovery_idle_seconds,
            notifier_worker=notifier_worker,
            extra_workers=open_interest_workers(
                self._wiring, self._settings, supervisor, admission
            ),
        )
        holder["runtime"] = runtime
        return runtime

    def _mark_fatal(self, exc: BaseException) -> None:
        self._status.mark_fatal(f"{type(exc).__name__}: {exc}")
        self._logger.exception("runtime failed")
