"""Focused HTTP handler for the canonical candle range endpoint."""

from __future__ import annotations

from market_data_service.adapters.http.consumer_read.exception_mapping import map_exception
from market_data_service.adapters.http.consumer_read.parsing import parse_request_target
from market_data_service.adapters.http.consumer_read.serialization import serialize_result
from market_data_service.application.consumer_read import GetCandleRange


class ConsumerReadHttpHandler:
    def __init__(self, query: GetCandleRange) -> None:
        self._query = query

    def handle(self, target: str) -> tuple[int, dict[str, object]]:
        try:
            return 200, serialize_result(self._query.execute(parse_request_target(target)))
        except Exception as exc:
            return map_exception(exc)
