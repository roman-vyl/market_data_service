"""Minimal JSON HTTP transport used by the Bybit adapter."""

from __future__ import annotations

import json
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from market_data_service.adapters.bybit.errors import BybitHttpError, BybitPayloadError

JsonObject = dict[str, Any]


class JsonHttpTransport(Protocol):
    def get_json(
        self,
        url: str,
        params: dict[str, str | int],
        timeout_seconds: float,
    ) -> JsonObject: ...


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
            with urlopen(request_url, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise BybitHttpError(
                f"GET {request_url} failed: {exc}",
                status_code=exc.code,
                retry_after_seconds=_retry_after_seconds(exc),
            ) from exc
        except (URLError, TimeoutError) as exc:
            raise BybitHttpError(f"GET {request_url} failed: {exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BybitPayloadError("Bybit response is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise BybitPayloadError("Bybit response root must be a JSON object")
        return payload


def _retry_after_seconds(exc: HTTPError) -> float | None:
    header = exc.headers.get("Retry-After")
    if header is None:
        return None
    stripped = header.strip()
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, IndexError):
        return None
    return max(0.0, retry_at.timestamp())
