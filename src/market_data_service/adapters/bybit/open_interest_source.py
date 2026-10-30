"""Bybit V5 REST adapter for bounded open interest windows."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from market_data_service.adapters.bybit.errors import BybitPayloadError
from market_data_service.adapters.bybit.rest_client import BybitRestCandleSource
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import OPEN_INTEREST_TIMEFRAME, ObservedOpenInterest
from market_data_service.domain.timeframes import get_timeframe, last_closed_open_time_ms
from market_data_service.domain.windows import TimeWindow

MAX_POINTS_PER_WINDOW = 200


@dataclass(slots=True)
class BybitOpenInterestSource:
    """Fetch closed-bar open interest points on the shared REST transport.

    Bybit includes both ``startTime`` and ``endTime`` and returns rows newest
    first; ``nextPageCursor`` is set even for a complete window, so windows are
    bounded to 200 grid points and the cursor is not used. With
    ``include_current`` the point of the bar open at the observation time is
    accepted too (the prefetch); later points are never accepted.
    """

    rest: BybitRestCandleSource
    include_current: bool = False

    def fetch_open_interest(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedOpenInterest, ...]:
        if stream.timeframe != OPEN_INTEREST_TIMEFRAME:
            raise ValueError("open interest is supported only for 5m streams")
        step_ms = get_timeframe(stream.timeframe).duration_ms
        points = window.duration_ms // step_ms
        if points > MAX_POINTS_PER_WINDOW:
            raise ValueError("Bybit open interest window must contain no more than 200 points")
        try:
            symbol = self.rest.exchange_symbols[stream.instrument.ticker]
        except KeyError as exc:
            raise ValueError(f"no Bybit symbol configured for {stream.instrument.ticker}") from exc
        rows = self.rest.get_market_rows(
            "/v5/market/open-interest",
            {
                "category": self.rest.category,
                "symbol": symbol,
                "intervalTime": "5min",
                "startTime": window.start_ms,
                "endTime": window.end_ms - step_ms,
                "limit": max(1, points),
            },
        )
        return parse_open_interest_rows(
            rows,
            stream=stream,
            requested_window=window,
            observed_at_ms=observed_at_ms,
            include_current=self.include_current,
        )


def parse_open_interest_rows(
    rows: Sequence[Any],
    *,
    stream: StreamKey,
    requested_window: TimeWindow,
    observed_at_ms: int,
    include_current: bool = False,
) -> tuple[ObservedOpenInterest, ...]:
    """Parse, keep admissible bars inside the window, de-duplicate and sort ascending.

    Admissible: closed bars; with ``include_current`` also the bar open now.
    """

    step_ms = get_timeframe(stream.timeframe).duration_ms
    last_closed = last_closed_open_time_ms(observed_at_ms, step_ms)
    newest = last_closed + step_ms if include_current else last_closed
    by_open_time: dict[int, ObservedOpenInterest] = {}
    for row in rows:
        point = _parse_row(row, stream=stream, observed_at_ms=observed_at_ms)
        if requested_window.contains(point.open_time_ms) and point.open_time_ms <= newest:
            by_open_time[point.open_time_ms] = point
    return tuple(by_open_time[key] for key in sorted(by_open_time))


def _parse_row(row: Any, *, stream: StreamKey, observed_at_ms: int) -> ObservedOpenInterest:
    if not isinstance(row, dict):
        raise BybitPayloadError("Bybit open interest row must be an object")
    value = row.get("openInterest")
    if not isinstance(value, str) or not value.strip():
        raise BybitPayloadError("Bybit open interest value must be a non-empty string")
    try:
        open_time_ms = int(row.get("timestamp"))  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise BybitPayloadError("Bybit open interest timestamp must be an integer") from exc
    return ObservedOpenInterest(
        stream=stream,
        open_time_ms=open_time_ms,
        open_interest=value.strip(),
        observed_at_ms=observed_at_ms,
    )
