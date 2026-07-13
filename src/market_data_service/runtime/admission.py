"""Per-stream admission gate before canonical realtime ingestion."""

from __future__ import annotations

from collections.abc import Iterable
from threading import RLock
from typing import Protocol

from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome
from market_data_service.domain.identity import StreamKey


class RealtimeEventHandler(Protocol):
    def handle(self, event: CandleObserved) -> RealtimeIngestionOutcome | None: ...


class RealtimeAdmissionGate:
    def __init__(self, admitted: Iterable[StreamKey] = ()) -> None:
        self._lock = RLock()
        self._admitted = set(admitted)

    def admit(self, stream: StreamKey) -> None:
        with self._lock:
            self._admitted.add(stream)

    def allows(self, stream: StreamKey) -> bool:
        with self._lock:
            return stream in self._admitted


class AdmissionGatedCandleHandler:
    def __init__(
        self,
        gate: RealtimeAdmissionGate,
        handler: RealtimeEventHandler,
    ) -> None:
        self._gate = gate
        self._handler = handler

    def handle(self, event: CandleObserved) -> RealtimeIngestionOutcome | None:
        if not self._gate.allows(event.stream):
            return None
        return self._handler.handle(event)
