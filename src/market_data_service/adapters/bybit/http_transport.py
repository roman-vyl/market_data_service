"""Minimal JSON HTTP transport used by the Bybit adapter."""

from __future__ import annotations

import json
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from market_data_service.adapters.bybit.errors import BybitHttpError, BybitPayloadError

JsonObject = dict[str, Any]


class JsonHttpTransport(Protocol):
    def get_json(self, url: str, params: dict[str, str | int], timeout_seconds: float) -> JsonObject: ...


class UrllibJsonHttpTransport:
    """Small standard-library transport; retry policy stays outside this class."""

    def get_json(
        self,
        url: str,
        params: dict[str, str | int],
        timeout_seconds: float,
    ) -> JsonObject:
        request_url = f"{url}?{urlencode(params)}"
        try:
            with urlopen(request_url, timeout=timeout_seconds) as response:  # noqa: S310
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            raise BybitHttpError(f"GET {request_url} failed: {exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BybitPayloadError("Bybit response is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise BybitPayloadError("Bybit response root must be a JSON object")
        return payload
