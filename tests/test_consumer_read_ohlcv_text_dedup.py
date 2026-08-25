"""Parity and access-count coverage for the ohlcv_text dedup fix.

Covers:
A. canonical_market_data_hash byte-for-byte parity against the pre-fix
   (naive, repeated-property-access) reference algorithm.
B. serialize_result response-dict parity against the same pre-fix reference.
C. ohlcv_text access-count regression guard (1 access per candle per pass),
   using a lightweight fake candle -- not a production instrumentation hook.
"""

from __future__ import annotations

import hashlib
import json

from market_data_service.adapters.http.consumer_read.serialization import serialize_result
from market_data_service.application.consumer_read.models import CandleRangeResult
from market_data_service.application.consumer_read.provenance import canonical_market_data_hash
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import InstrumentKey, StreamKey


def _stream() -> StreamKey:
    return StreamKey(InstrumentKey("ETHUSDT.P"), "5m")


def _candles(stream: StreamKey, count: int) -> tuple[CanonicalCandle, ...]:
    return tuple(
        CanonicalCandle(
            stream=stream,
            open_time_ms=index * 300_000,
            close_time_ms=index * 300_000 + 299_999,
            open=f"{1000 + index}.5",
            high=f"{1001 + index}.25",
            low=f"{999 + index}.125",
            close=f"{1000 + index}.75",
            volume=f"{10 + index}.000001",
            source=ObservationSource.BYBIT_REST,
            committed_at_ms=index * 300_000 + 1_000,
        )
        for index in range(count)
    )


def _reference_hash(
    *, stream: StreamKey, from_ms: int, to_ms: int, candles: tuple[CanonicalCandle, ...]
) -> str:
    """Pre-fix reference: repeated (5x per candle) ohlcv_text property access."""
    document = {
        "ticker": stream.instrument.ticker,
        "timeframe": stream.timeframe,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "candles": [
            {
                "open_time_ms": candle.open_time_ms,
                "open": candle.ohlcv_text[0],
                "high": candle.ohlcv_text[1],
                "low": candle.ohlcv_text[2],
                "close": candle.ohlcv_text[3],
                "volume": candle.ohlcv_text[4],
            }
            for candle in candles
        ],
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _reference_serialize(result: CandleRangeResult) -> dict[str, object]:
    """Pre-fix reference: repeated (5x per candle) ohlcv_text property access."""
    return {
        "ticker": result.stream.instrument.ticker,
        "timeframe": result.stream.timeframe,
        "from_ms": result.from_ms,
        "to_ms": result.to_ms,
        "market_data_hash": result.market_data_hash,
        "candles": [
            {
                "open_time_ms": candle.open_time_ms,
                "open": candle.ohlcv_text[0],
                "high": candle.ohlcv_text[1],
                "low": candle.ohlcv_text[2],
                "close": candle.ohlcv_text[3],
                "volume": candle.ohlcv_text[4],
            }
            for candle in result.candles
        ],
    }


def test_canonical_market_data_hash_byte_for_byte_parity_empty() -> None:
    stream = _stream()
    candles = _candles(stream, 0)
    expected = _reference_hash(stream=stream, from_ms=0, to_ms=0, candles=candles)
    actual = canonical_market_data_hash(stream=stream, from_ms=0, to_ms=0, candles=candles)
    assert actual == expected


def test_canonical_market_data_hash_byte_for_byte_parity_single_candle() -> None:
    stream = _stream()
    candles = _candles(stream, 1)
    from_ms, to_ms = 0, 300_000
    expected = _reference_hash(stream=stream, from_ms=from_ms, to_ms=to_ms, candles=candles)
    actual = canonical_market_data_hash(
        stream=stream, from_ms=from_ms, to_ms=to_ms, candles=candles
    )
    assert actual == expected


def test_canonical_market_data_hash_byte_for_byte_parity_multiple_candles() -> None:
    stream = _stream()
    candles = _candles(stream, 25)
    from_ms, to_ms = 0, 25 * 300_000
    expected = _reference_hash(stream=stream, from_ms=from_ms, to_ms=to_ms, candles=candles)
    actual = canonical_market_data_hash(
        stream=stream, from_ms=from_ms, to_ms=to_ms, candles=candles
    )
    assert actual == expected


def test_serialize_result_exact_parity_empty() -> None:
    stream = _stream()
    candles = _candles(stream, 0)
    result = CandleRangeResult(
        stream=stream, from_ms=0, to_ms=0, market_data_hash="fixture-hash", candles=candles
    )
    assert serialize_result(result) == _reference_serialize(result)


def test_serialize_result_exact_parity_multiple_candles() -> None:
    stream = _stream()
    candles = _candles(stream, 25)
    result = CandleRangeResult(
        stream=stream,
        from_ms=0,
        to_ms=25 * 300_000,
        market_data_hash="fixture-hash",
        candles=candles,
    )
    expected = _reference_serialize(result)
    actual = serialize_result(result)
    assert actual == expected
    # exact string values, not just structural equality
    for expected_candle, actual_candle in zip(expected["candles"], actual["candles"], strict=True):  # type: ignore[index]
        assert expected_candle == actual_candle


class _CountingFakeCandle:
    """Minimal fake exposing exactly the interface _candle_document() uses
    (open_time_ms, ohlcv_text) -- not a CanonicalCandle instrumentation hook,
    just a test double standing in for one."""

    def __init__(self, index: int) -> None:
        self.open_time_ms = index * 300_000
        self.access_count = 0
        self._values = (
            f"{1000 + index}.5",
            f"{1001 + index}.25",
            f"{999 + index}.125",
            f"{1000 + index}.75",
            f"{10 + index}.000001",
        )

    @property
    def ohlcv_text(self) -> tuple[str, str, str, str, str]:
        self.access_count += 1
        return self._values


def test_provenance_accesses_ohlcv_text_exactly_once_per_candle() -> None:
    stream = _stream()
    fakes = [_CountingFakeCandle(index) for index in range(10)]
    canonical_market_data_hash(
        stream=stream,
        from_ms=0,
        to_ms=10 * 300_000,
        candles=fakes,  # type: ignore[arg-type]
    )
    assert all(fake.access_count == 1 for fake in fakes)


def test_serialization_accesses_ohlcv_text_exactly_once_per_candle() -> None:
    stream = _stream()
    fakes = [_CountingFakeCandle(index) for index in range(10)]
    result = CandleRangeResult(
        stream=stream,
        from_ms=0,
        to_ms=10 * 300_000,
        market_data_hash="fixture-hash",
        candles=fakes,  # type: ignore[arg-type]
    )
    serialize_result(result)
    assert all(fake.access_count == 1 for fake in fakes)
