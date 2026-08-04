"""Thread-safe runtime health and readiness projection."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from threading import RLock

from market_data_service.application.realtime.supervisor_types import RealtimeStreamFacts
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamStateSnapshot


@dataclass(frozen=True, slots=True)
class RuntimeStreamStatus:
    stream: str
    durable_state: str
    realtime_status: str
    data_ready: bool
    realtime_live: bool
    ready: bool
    reason: str | None


class RuntimeStatusStore:
    def __init__(self, streams: tuple[StreamKey, ...]) -> None:
        self._lock = RLock()
        self._logger = logging.getLogger("market_data_service.runtime.status")
        self._healthy = False
        self._fatal_error: str | None = None
        self._aggregate_ready = False
        self._blocking_reasons: dict[StreamKey, str] = {}
        self._streams = {
            stream: RuntimeStreamStatus(
                stream=stream.canonical_id,
                durable_state="uninitialized",
                realtime_status="expected",
                data_ready=False,
                realtime_live=False,
                ready=False,
                reason="startup_pending",
            )
            for stream in streams
        }

    def mark_healthy(self) -> None:
        with self._lock:
            changed = not self._healthy or self._fatal_error is not None
            self._healthy = True
            self._fatal_error = None
        if changed:
            self._logger.info("process health status=healthy fatal_error=None")

    def mark_fatal(self, detail: str) -> None:
        with self._lock:
            changed = self._healthy or self._fatal_error != detail
            self._healthy = False
            self._fatal_error = detail
        if changed:
            self._logger.error("process health status=unhealthy fatal_error=%s", detail)

    def update_stream(
        self,
        durable: StreamStateSnapshot,
        realtime: RealtimeStreamFacts | None,
    ) -> None:
        with self._lock:
            realtime_status = "not_started" if realtime is None else realtime.status.value
            data_ready = bool(durable.is_ready and realtime is not None and realtime.data_ready)
            realtime_live = bool(realtime is not None and realtime.realtime_live)
            ready = data_ready
            override = self._blocking_reasons.get(durable.stream)
            reason = None if ready else (override or self._reason(durable, realtime))
            status = RuntimeStreamStatus(
                stream=durable.stream.canonical_id,
                durable_state=durable.state.value,
                realtime_status=realtime_status,
                data_ready=data_ready,
                realtime_live=realtime_live,
                ready=ready,
                reason=reason,
            )
            stream_changed, readiness_changed = self._replace_stream_locked(
                durable.stream,
                status,
            )
        self._log_transitions(status, stream_changed, readiness_changed)

    def set_blocking_reason(self, stream: StreamKey, reason: str) -> None:
        with self._lock:
            self._blocking_reasons[stream] = reason
            current = self._streams[stream]
            status = RuntimeStreamStatus(
                stream=current.stream,
                durable_state=current.durable_state,
                realtime_status=current.realtime_status,
                data_ready=False,
                realtime_live=current.realtime_live,
                ready=False,
                reason=reason,
            )
            stream_changed, readiness_changed = self._replace_stream_locked(stream, status)
        self._log_transitions(status, stream_changed, readiness_changed)

    def clear_blocking_reason(self, stream: StreamKey) -> None:
        with self._lock:
            self._blocking_reasons.pop(stream, None)

    def health_document(self) -> dict[str, object]:
        with self._lock:
            return {
                "status": "healthy" if self._healthy else "unhealthy",
                "fatal_error": self._fatal_error,
            }

    def readiness_document(self) -> dict[str, object]:
        with self._lock:
            streams = tuple(self._streams.values())
            return {
                "ready": self._aggregate_ready,
                "streams": [asdict(item) for item in streams],
            }

    @property
    def healthy(self) -> bool:
        with self._lock:
            return self._healthy

    @property
    def ready(self) -> bool:
        return bool(self.readiness_document()["ready"])

    def _replace_stream_locked(
        self,
        stream: StreamKey,
        status: RuntimeStreamStatus,
    ) -> tuple[bool, tuple[bool, int, int] | None]:
        previous = self._streams[stream]
        previous_ready = self._aggregate_ready
        self._streams[stream] = status
        self._aggregate_ready = bool(self._streams) and all(
            item.ready for item in self._streams.values()
        )
        readiness_changed = (
            (
                self._aggregate_ready,
                sum(item.ready for item in self._streams.values()),
                len(self._streams),
            )
            if self._aggregate_ready != previous_ready
            else None
        )
        return status != previous, readiness_changed

    def _log_transitions(
        self,
        status: RuntimeStreamStatus,
        stream_changed: bool,
        readiness_changed: tuple[bool, int, int] | None,
    ) -> None:
        if stream_changed:
            self._logger.info(
                "stream status stream=%s durable_state=%s realtime_status=%s "
                "data_ready=%s realtime_live=%s ready=%s reason=%s",
                status.stream,
                status.durable_state,
                status.realtime_status,
                status.data_ready,
                status.realtime_live,
                status.ready,
                status.reason,
            )
        if readiness_changed is not None:
            self._logger.info(
                "service readiness ready=%s ready_streams=%s total_streams=%s",
                *readiness_changed,
            )

    @staticmethod
    def _reason(
        durable: StreamStateSnapshot,
        realtime: RealtimeStreamFacts | None,
    ) -> str:
        if durable.last_error_code:
            return durable.last_error_code
        if not durable.is_ready:
            return durable.state.value
        if realtime is None:
            return "realtime_not_started"
        if realtime.fatal_error_code:
            return realtime.fatal_error_code
        if realtime.recovery_pending:
            return "recovery_pending"
        if not realtime.subscription_active:
            return "subscription_inactive"
        if not realtime.recovery_restored:
            return "recovery_not_restored"
        return realtime.status.value
