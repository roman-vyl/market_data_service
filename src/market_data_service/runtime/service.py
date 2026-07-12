"""Long-running market-data service process composition."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

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
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import StartupClassification
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
            eligible = self._startup()
            self._status.mark_healthy()
            if not eligible:
                await stop_event.wait()
                return
            runtime = self._build_realtime(eligible)
            await runtime.run(stop_event)
        except Exception as exc:
            self._status.mark_fatal(f"{type(exc).__name__}: {exc}")
            self._logger.exception("runtime failed")
            raise
        finally:
            self._http_server.close()

    def _startup(self) -> tuple[StreamKey, ...]:
        coordinator = StartupCoordinator(
            bootstrap_factory=self._wiring.bootstrap,
            auditor=self._wiring.auditor(),
            repair=self._wiring.repair(),
            lifecycle=self._wiring.lifecycle(),
            backfill_windows_per_stream=(
                self._settings.startup_backfill_windows_per_stream
            ),
            repair_windows_per_stream=self._settings.startup_repair_windows_per_stream,
        )
        outcomes = coordinator.execute(self._config.enabled_streams)
        eligible: list[StreamKey] = []
        lifecycle = self._wiring.lifecycle()
        for outcome in outcomes:
            snapshot = lifecycle.snapshot(outcome.stream)
            self._status.update_stream(snapshot, None)
            self._logger.info(
                "startup stream=%s classification=%s",
                outcome.stream.canonical_id,
                outcome.classification.value,
            )
            if outcome.classification is StartupClassification.CONNECTING:
                eligible.append(outcome.stream)
        return tuple(eligible)

    def _build_realtime(
        self,
        streams: Sequence[StreamKey],
    ) -> RuntimeRealtimeCoordinator:
        full_map = BybitTopicMap.from_config(self._config)
        allowed = set(streams)
        topic_map = BybitTopicMap(
            {
                topic: stream
                for topic, stream in full_map.topic_to_stream.items()
                if stream in allowed
            }
        )
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
        holder: dict[str, RuntimeRealtimeCoordinator] = {}

        async def on_event(event: object) -> None:
            await holder["runtime"].on_event(event)  # type: ignore[arg-type]

        async def on_outcome(outcome: object) -> None:
            await holder["runtime"].on_outcome(outcome)  # type: ignore[arg-type]

        connector = RealtimeConnector(
            url=self._settings.websocket_url,
            transport=WebsocketsTransport(),
            adapter=BybitWebSocketAdapter(topic_map),
            candle_handler=self._wiring.candle_handler(),
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
            now_ms=self._wiring.clock.now_ms,
            max_backfill_windows=self._settings.startup_backfill_windows_per_stream,
            max_repair_windows=self._settings.startup_repair_windows_per_stream,
        )
        holder["runtime"] = runtime
        return runtime
