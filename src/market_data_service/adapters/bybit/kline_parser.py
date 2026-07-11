"""Normalize Bybit V5 kline rows into transport-neutral candle observations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from market_data_service.adapters.bybit.errors import BybitPayloadError
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow


def parse_kline_rows(
    rows: Sequence[Any],
    *,
    stream: StreamKey,
    requested_window: TimeWindow,
    observed_at_ms: int,
) -> tuple[ObservedCandle, ...]:
    """Parse, filter, de-duplicate, and sort Bybit kline rows ascending."""

    step_ms = get_timeframe(stream.timeframe).duration_ms
    by_open_time: dict[int, ObservedCandle] = {}
    for row in rows:
        candle = _parse_row(row, stream=stream, observed_at_ms=observed_at_ms, step_ms=step_ms)
        if requested_window.contains(candle.open_time_ms):
            by_open_time[candle.open_time_ms] = candle
    return tuple(by_open_time[key] for key in sorted(by_open_time))


def _parse_row(
    row: Any,
    *,
    stream: StreamKey,
    observed_at_ms: int,
    step_ms: int,
) -> ObservedCandle:
    if not isinstance(row, list) or len(row) < 6:
        raise BybitPayloadError("Bybit kline row must be a list with at least 6 fields")
    try:
        open_time_ms = int(row[0])
    except (TypeError, ValueError) as exc:
        raise BybitPayloadError("Bybit kline start time must be an integer") from exc
    close_time_ms = open_time_ms + step_ms - 1
    return ObservedCandle(
        stream=stream,
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        open=row[1],
        high=row[2],
        low=row[3],
        close=row[4],
        volume=row[5],
        confirmed=close_time_ms < observed_at_ms,
        observed_at_ms=observed_at_ms,
        source=ObservationSource.BYBIT_REST,
    )
