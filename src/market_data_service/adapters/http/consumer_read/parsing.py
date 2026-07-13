"""Parse the consumer range query string."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from market_data_service.application.consumer_read.errors import InvalidRange
from market_data_service.application.consumer_read.models import CandleRangeRequest


def parse_request_target(target: str) -> CandleRangeRequest:
    query = parse_qs(urlsplit(target).query, keep_blank_values=True)
    allowed = {"ticker", "timeframe", "from_ms", "to_ms"}
    if set(query) - allowed:
        raise InvalidRange("unsupported query parameter")
    values: dict[str, str] = {}
    for name in allowed:
        items = query.get(name)
        if items is None or len(items) != 1 or not items[0]:
            raise InvalidRange(f"{name} is required exactly once")
        values[name] = items[0]
    try:
        from_ms = int(values["from_ms"])
        to_ms = int(values["to_ms"])
    except ValueError as exc:
        raise InvalidRange("from_ms and to_ms must be integers") from exc
    return CandleRangeRequest(
        ticker=values["ticker"],
        timeframe=values["timeframe"],
        from_ms=from_ms,
        to_ms=to_ms,
    )
