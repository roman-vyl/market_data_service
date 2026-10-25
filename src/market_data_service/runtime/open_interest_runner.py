"""Background owner of the open interest track inside the running service.

Runs after candle startup, shares the REST operation gate, yields it to other
work, reports only to logs and track progress, and contains every failure so
the candle pipeline, readiness and the service keep running.
"""

from __future__ import annotations

import asyncio
import logging

from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.open_interest_reconciliation import OpenInterestTrackReconciler
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup_types import StartupClassification, StartupStreamOutcome
from market_data_service.runtime.wiring import RuntimeWiring

_LOGGER = logging.getLogger("market_data_service.runtime.oi_track")
_RESTART_DELAY_SECONDS = 60.0


class LogOnlyBlockingReasons:
    """Blocking reasons of the open interest track go to logs, never to readiness."""

    def set_blocking_reason(self, stream: StreamKey, reason: str) -> None:
        _LOGGER.debug("oi track stream=%s state=%s", stream.canonical_id, reason)

    def clear_blocking_reason(self, stream: StreamKey) -> None:
        return None


class OpenInterestTrackRunner:
    def __init__(
        self,
        *,
        streams: tuple[StreamKey, ...],
        wiring: RuntimeWiring,
        settings: RuntimeSettings,
        operation_gate: asyncio.Lock,
        restart_delay_seconds: float = _RESTART_DELAY_SECONDS,
    ) -> None:
        self._streams = streams
        self._wiring = wiring
        self._settings = settings
        self._operation_gate = operation_gate
        self._restart_delay_seconds = restart_delay_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self._streams:
            return
        _LOGGER.info(
            "oi track start streams=%s",
            ",".join(stream.canonical_id for stream in self._streams),
        )
        while not stop_event.is_set():
            try:
                await self._worker().run(stop_event)
            except Exception:
                _LOGGER.exception(
                    "oi track worker failed; restarting in %s s", self._restart_delay_seconds
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._restart_delay_seconds)
                except TimeoutError:
                    continue

    def _worker(self) -> HistoricalReconciliationWorker:
        wiring = self._wiring
        reconciler = OpenInterestTrackReconciler(
            lower_bound=wiring.open_interest_lower_bound(),
            repair=wiring.open_interest_repair(),
            state=wiring.open_interest_state(),
            now_ms=wiring.clock.now_ms,
            discovery_windows_per_pass=self._settings.oi_track_windows_per_pass,
            repair_windows_per_pass=self._settings.oi_track_windows_per_pass,
        )
        holder: dict[str, HistoricalReconciliationWorker] = {}

        async def on_complete(stream: StreamKey) -> None:
            holder["worker"].schedule(
                stream, delay_seconds=self._settings.oi_track_refresh_seconds
            )

        worker = HistoricalReconciliationWorker(
            coordinator=reconciler,
            initial_outcomes=tuple(
                StartupStreamOutcome(stream, StartupClassification.INCOMPLETE)
                for stream in self._streams
            ),
            status=LogOnlyBlockingReasons(),
            operation_gate=self._operation_gate,
            on_complete=on_complete,
            base_backoff_seconds=self._settings.historical_retry_base_seconds,
            max_backoff_seconds=self._settings.historical_retry_max_seconds,
            label="oi track",
            yield_to_gate=True,
        )
        holder["worker"] = worker
        return worker
