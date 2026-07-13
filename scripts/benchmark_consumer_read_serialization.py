"""Synthetic benchmark for unpaginated consumer-read JSON serialization."""

from __future__ import annotations

import json
import time
from decimal import Decimal

from market_data_service.adapters.http.consumer_read.serialization import serialize_result
from market_data_service.application.consumer_read.models import CandleRangeResult
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import InstrumentKey, StreamKey


def run(count: int) -> tuple[float, int]:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
    candles = tuple(
        CanonicalCandle(
            stream,
            index * 300_000,
            index * 300_000 + 299_999,
            Decimal("68450.10"),
            Decimal("68520.00"),
            Decimal("68390.40"),
            Decimal("68480.70"),
            Decimal("123.456"),
            ObservationSource.BYBIT_REST,
            1,
        )
        for index in range(count)
    )
    started = time.perf_counter()
    payload = json.dumps(
        serialize_result(CandleRangeResult(stream, 0, count * 300_000, candles)),
        separators=(",", ":"),
    ).encode()
    return time.perf_counter() - started, len(payload)


if __name__ == "__main__":
    for size in (1_000, 10_000, 100_000):
        elapsed, bytes_count = run(size)
        print(f"{size}: {elapsed:.3f}s, {bytes_count / 1024 / 1024:.2f} MiB")
