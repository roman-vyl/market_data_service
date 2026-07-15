"""Small transport router for runtime HTTP requests."""

from __future__ import annotations

from typing import Protocol
from urllib.parse import urlsplit

from market_data_service.adapters.http.consumer_read import ConsumerReadHttpHandler
from market_data_service.adapters.http.consumer_read.openapi import openapi_document
from market_data_service.adapters.http.historical_read import HistoricalReadHttpHandler
from market_data_service.adapters.http.history_planning import HistoryPlanningHttpHandler


class RuntimeStatusView(Protocol):
    @property
    def healthy(self) -> bool: ...

    @property
    def ready(self) -> bool: ...

    def health_document(self) -> dict[str, object]: ...

    def readiness_document(self) -> dict[str, object]: ...


class RuntimeRequestRouter:
    def __init__(
        self,
        status: RuntimeStatusView,
        consumer_read: ConsumerReadHttpHandler | None,
        history_planning: HistoryPlanningHttpHandler | None,
        historical_read: HistoricalReadHttpHandler | None,
    ) -> None:
        self._status = status
        self._consumer_read = consumer_read
        self._history_planning = history_planning
        self._historical_read = historical_read

    def get(self, target: str) -> tuple[int, dict[str, object]]:
        if target == "/health":
            document = self._status.health_document()
            return (200 if self._status.healthy else 503), document
        if target == "/readiness":
            document = self._status.readiness_document()
            return (200 if self._status.ready else 503), document
        path = urlsplit(target).path
        if path == "/openapi.json":
            return 200, openapi_document()
        if path == "/v1/candles" and self._consumer_read is not None:
            return self._consumer_read.handle(target)
        if self._history_planning is not None and self._history_planning.matches(target):
            return self._history_planning.handle_get(target)
        return 404, {"error": "not_found"}

    def post(self, target: str, payload: bytes) -> tuple[int, dict[str, object]]:
        if urlsplit(target).path == "/v1/historical-candles" and self._historical_read is not None:
            return self._historical_read.handle(payload)
        if self._history_planning is not None and self._history_planning.matches(target):
            return self._history_planning.handle_post(target, payload)
        return 404, {"error": "not_found"}
