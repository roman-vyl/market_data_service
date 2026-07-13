"""Thread-safe runtime health and readiness projection."""

from __future__ import annotations

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
        self._healthy = False
        self._fatal_error: str | None = None
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
            self._healthy = True
            self._fatal_error = None

    def mark_fatal(self, detail: str) -> None:
        with self._lock:
            self._healthy = False
            self._fatal_error = detail

    def update_stream(
        self,
        durable: StreamStateSnapshot,
        realtime: RealtimeStreamFacts | None,
    ) -> None:
        realtime_status = "not_started" if realtime is None else realtime.status.value
        data_ready = bool(durable.is_ready and realtime is not None and realtime.data_ready)
        realtime_live = bool(realtime is not None and realtime.realtime_live)
        ready = data_ready
        override = self._blocking_reasons.get(durable.stream)
        reason = None if ready else (override or self._reason(durable, realtime))
        with self._lock:
            self._streams[durable.stream] = RuntimeStreamStatus(
                stream=durable.stream.canonical_id,
                durable_state=durable.state.value,
                realtime_status=realtime_status,
                data_ready=data_ready,
                realtime_live=realtime_live,
                ready=ready,
                reason=reason,
            )


    def set_blocking_reason(self, stream: StreamKey, reason: str) -> None:
        with self._lock:
            self._blocking_reasons[stream] = reason
            current = self._streams[stream]
            self._streams[stream] = RuntimeStreamStatus(
                stream=current.stream,
                durable_state=current.durable_state,
                realtime_status=current.realtime_status,
                data_ready=False,
                realtime_live=current.realtime_live,
                ready=False,
                reason=reason,
            )

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
            ready = bool(streams) and all(item.ready for item in streams)
            return {
                "ready": ready,
                "streams": [asdict(item) for item in streams],
            }

    @property
    def healthy(self) -> bool:
        with self._lock:
            return self._healthy

    @property
    def ready(self) -> bool:
        return bool(self.readiness_document()["ready"])

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
