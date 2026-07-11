from __future__ import annotations

from decimal import Decimal

import pytest

from market_data_service.domain import (
    CanonicalCandle,
    Gap,
    InstrumentKey,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    TimeWindow,
    align_to_grid,
    ceil_to_grid,
    find_gaps,
    get_timeframe,
    iter_fetch_windows,
    last_closed_open_time_ms,
)


def test_identity_normalizes_stable_fields() -> None:
    instrument = InstrumentKey(" btcusdt.p ")
    stream = StreamKey(instrument, "1M")
    assert instrument.canonical_id == "BTCUSDT.P"
    assert stream.canonical_id == "BTCUSDT.P:1m"


def test_mandatory_minute_timeframe_and_grid_semantics() -> None:
    minute = get_timeframe("1m")
    assert minute.duration_ms == 60_000
    assert minute.bybit_interval == "1"
    assert align_to_grid(125_999, minute.duration_ms) == 120_000
    assert ceil_to_grid(120_001, minute.duration_ms) == 180_000
    assert last_closed_open_time_ms(180_000, minute.duration_ms) == 120_000


def test_half_open_window_excludes_end() -> None:
    window = TimeWindow(60_000, 180_000)
    assert window.contains(60_000)
    assert window.contains(179_999)
    assert not window.contains(180_000)


def test_gap_detector_sorts_deduplicates_and_merges() -> None:
    gaps = find_gaps(
        [180_000, 60_000, 60_000, 300_000],
        audit_window=TimeWindow(60_000, 360_000),
        step_ms=60_000,
    )
    assert gaps == (Gap(120_000, 180_000), Gap(240_000, 300_000))


def test_gap_detector_rejects_off_grid_data() -> None:
    with pytest.raises(ValueError, match="off-grid"):
        find_gaps(
            [60_001],
            audit_window=TimeWindow(60_000, 120_000),
            step_ms=60_000,
        )


def test_fetch_windows_are_bounded_and_half_open() -> None:
    windows = iter_fetch_windows(
        Gap(0, 7 * 60_000),
        step_ms=60_000,
        max_candles=3,
    )
    assert windows == (
        TimeWindow(0, 180_000),
        TimeWindow(180_000, 360_000),
        TimeWindow(360_000, 420_000),
    )


def test_observed_and_canonical_candles_are_distinct_contracts() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    observed = ObservedCandle(
        stream=stream,
        open_time_ms=60_000,
        close_time_ms=119_999,
        open=Decimal("100.0"),
        high=Decimal("101"),
        low=Decimal("99.5"),
        close=Decimal("100.25"),
        volume=Decimal("12.3400"),
        confirmed=True,
        observed_at_ms=120_010,
        source=ObservationSource.BYBIT_REST,
    )
    canonical = CanonicalCandle(
        stream=observed.stream,
        open_time_ms=observed.open_time_ms,
        close_time_ms=observed.close_time_ms,
        open=observed.open,
        high=observed.high,
        low=observed.low,
        close=observed.close,
        volume=observed.volume,
        source=observed.source,
        committed_at_ms=120_020,
    )
    assert observed.confirmed is True
    assert not hasattr(canonical, "confirmed")
