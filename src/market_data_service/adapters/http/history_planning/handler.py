"""HTTP adapter for read-only research history planning contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from market_data_service.adapters.http.error_envelope import map_exception
from market_data_service.application.audit_continuity import UnknownStreamError
from market_data_service.application.history_planning import AuditStreamRange, GetStreamBounds
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import InstrumentKey, StreamKey


@dataclass(frozen=True, slots=True)
class _Route:
    stream: StreamKey
    operation: str


class HistoryPlanningHttpHandler:
    def __init__(
        self,
        config: ValidatedMarketConfig,
        bounds: GetStreamBounds,
        audit: AuditStreamRange,
    ) -> None:
        self._configured = frozenset(config.enabled_streams)
        self._bounds = bounds
        self._audit = audit

    def handle_get(self, target: str) -> tuple[int, dict[str, object]]:
        try:
            route = self._parse_route(target)
            if route.operation != "bounds":
                return 404, {"error": "not_found", "detail": "no such planning operation"}
            result = self._bounds.execute(route.stream)
            return 200, {
                "contract_version": "market_stream_bounds.v1",
                "ticker": result.stream.instrument.ticker,
                "timeframe": result.stream.timeframe,
                "state": result.state,
                "earliest_committed_open_time_ms": (result.earliest_committed_open_time_ms),
                "latest_committed_open_time_ms": result.latest_committed_open_time_ms,
            }
        except Exception as exc:
            return map_exception(exc)

    def handle_post(
        self,
        target: str,
        payload: bytes,
    ) -> tuple[int, dict[str, object]]:
        try:
            route = self._parse_route(target)
            if route.operation != "continuity-audits":
                return 404, {"error": "not_found", "detail": "no such planning operation"}
            request = self._parse_audit_payload(payload)
            result = self._audit.execute(
                route.stream,
                start_time_ms=request["from_ms"],
                end_time_ms=request["to_ms"],
            )
            return 200, {
                "contract_version": "market_continuity_audit.v1",
                "ticker": result.stream.instrument.ticker,
                "timeframe": result.stream.timeframe,
                "checked_start_ms": result.checked_start_ms,
                "checked_end_ms": result.checked_end_ms,
                "candle_count": result.candle_count,
                "is_continuous": result.is_continuous,
                "gaps": [{"from_ms": gap.start_ms, "to_ms": gap.end_ms} for gap in result.gaps],
                "state": result.state,
                "market_data_hash": result.market_data_hash,
            }
        except Exception as exc:
            return map_exception(exc)

    def matches(self, target: str) -> bool:
        path = urlsplit(target).path
        parts = path.strip("/").split("/")
        return len(parts) == 5 and parts[:2] == ["v1", "streams"]

    def _parse_route(self, target: str) -> _Route:
        path = urlsplit(target).path
        parts = path.strip("/").split("/")
        if len(parts) != 5 or parts[:2] != ["v1", "streams"]:
            raise LookupError("route not found")
        ticker, timeframe, operation = map(unquote, parts[2:])
        try:
            stream = StreamKey(InstrumentKey(ticker), timeframe)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        if stream not in self._configured:
            raise UnknownStreamError(stream.canonical_id)
        return _Route(stream=stream, operation=operation)

    @staticmethod
    def _parse_audit_payload(payload: bytes) -> dict[str, int]:
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(document, dict):
            raise ValueError("request body must be a JSON object")
        if set(document) != {"from_ms", "to_ms"}:
            raise ValueError("request body must contain only from_ms/to_ms")
        start = document["from_ms"]
        end = document["to_ms"]
        if isinstance(start, bool) or not isinstance(start, int):
            raise ValueError("from_ms must be an integer")
        if isinstance(end, bool) or not isinstance(end, int):
            raise ValueError("to_ms must be an integer")
        return {"from_ms": start, "to_ms": end}
