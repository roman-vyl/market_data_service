"""HTTP handler for hash-bound historical candle reads."""

from __future__ import annotations

import json

from market_data_service.adapters.http.consumer_read.exception_mapping import map_exception
from market_data_service.adapters.http.consumer_read.serialization import serialize_result
from market_data_service.application.consumer_read import (
    GetHistoricalCandleRange,
    HistoricalCandleRangeRequest,
)


class HistoricalReadHttpHandler:
    def __init__(self, query: GetHistoricalCandleRange) -> None:
        self._query = query

    def handle(self, payload: bytes) -> tuple[int, dict[str, object]]:
        try:
            document = json.loads(payload)
            if not isinstance(document, dict):
                raise ValueError("request body must be a JSON object")
            expected = {
                "ticker",
                "timeframe",
                "from_ms",
                "to_ms",
                "expected_market_data_hash",
            }
            if set(document) != expected:
                raise ValueError("request body has unexpected or missing fields")
            request = HistoricalCandleRangeRequest(
                ticker=self._require_str(document, "ticker"),
                timeframe=self._require_str(document, "timeframe"),
                from_ms=self._require_int(document, "from_ms"),
                to_ms=self._require_int(document, "to_ms"),
                expected_market_data_hash=self._require_str(document, "expected_market_data_hash"),
            )
            return 200, serialize_result(self._query.execute(request))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return 422, {"error": "invalid_request", "detail": str(exc)}
        except Exception as exc:
            from market_data_service.application.consumer_read.errors import ConsumerReadError

            if isinstance(exc, ConsumerReadError):
                return map_exception(exc)
            if isinstance(exc, ValueError):
                return 422, {"error": "invalid_request", "detail": str(exc)}
            return map_exception(exc)

    @staticmethod
    def _require_str(document: dict[str, object], key: str) -> str:
        value = document[key]
        if not isinstance(value, str) or not value:
            raise ValueError(f"{key} must be a non-empty string")
        return value

    @staticmethod
    def _require_int(document: dict[str, object], key: str) -> int:
        value = document[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{key} must be an integer")
        return value
