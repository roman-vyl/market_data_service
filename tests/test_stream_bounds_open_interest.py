"""Bounds of a stream with an open interest track (mds-stream-bounds-open-interest-v1)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.history_planning import GetStreamBounds
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.open_interest import (
    OPEN_INTEREST_TRACK,
    CanonicalOpenInterest,
    TrackProgress,
)
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.domain.timeframes import get_timeframe

INSTRUMENT = InstrumentKey("BTCUSDT.P")
OI_STREAM = StreamKey(INSTRUMENT, "5m")
HOUR_STREAM = StreamKey(INSTRUMENT, "1h")
STEP = 300_000


def _database(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    register_stream(database, OI_STREAM, exchange_symbol="BTCUSDT", now_ms=1)
    register_stream(database, HOUR_STREAM, exchange_symbol="BTCUSDT", now_ms=1)
    return database


def _candles(database: Path, stream: StreamKey, open_times_ms: tuple[int, ...]) -> None:
    step = get_timeframe(stream.timeframe).duration_ms
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    for open_time_ms in open_times_ms:
        ingest.execute(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + step - 1,
                open="1",
                high="2",
                low="1",
                close="1.5",
                volume="10",
                confirmed=True,
                observed_at_ms=open_time_ms + step,
                source=ObservationSource.BYBIT_REST,
            ),
            committed_at_ms=open_time_ms + step + 1,
        )


def _open_interest(
    database: Path,
    *,
    lower_bound_ms: int | None,
    open_times_ms: tuple[int, ...],
) -> None:
    with SqliteUnitOfWork(database) as unit_of_work:
        for open_time_ms in open_times_ms:
            unit_of_work.save_open_interest(
                CanonicalOpenInterest(OI_STREAM, open_time_ms, "100", open_time_ms + 1)
            )
        unit_of_work.save_track_progress(
            TrackProgress(
                OI_STREAM,
                OPEN_INTEREST_TRACK,
                earliest_available_open_time_ms=lower_bound_ms,
                latest_committed_open_time_ms=max(open_times_ms) if open_times_ms else None,
            )
        )
        unit_of_work.commit()


def _bounds(database: Path) -> GetStreamBounds:
    return GetStreamBounds(lambda: SqliteUnitOfWork(database), (OI_STREAM,))


def test_stream_without_open_interest_track_keeps_candle_bounds(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, HOUR_STREAM, (0, 3_600_000, 7_200_000))
    _candles(database, OI_STREAM, (0, STEP, 2 * STEP))

    hour = _bounds(database).execute(HOUR_STREAM)
    unflagged = GetStreamBounds(lambda: SqliteUnitOfWork(database)).execute(OI_STREAM)

    assert (hour.earliest_committed_open_time_ms, hour.latest_committed_open_time_ms) == (
        0,
        7_200_000,
    )
    assert (
        unflagged.earliest_committed_open_time_ms,
        unflagged.latest_committed_open_time_ms,
    ) == (0, 2 * STEP)


def test_open_interest_starting_later_moves_earliest(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, tuple(index * STEP for index in range(6)))
    _open_interest(
        database, lower_bound_ms=2 * STEP, open_times_ms=(2 * STEP, 3 * STEP, 4 * STEP, 5 * STEP)
    )

    result = _bounds(database).execute(OI_STREAM)

    assert result.earliest_committed_open_time_ms == 2 * STEP
    assert result.latest_committed_open_time_ms == 5 * STEP


def test_candle_committed_before_its_open_interest_limits_latest(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, (0, STEP, 2 * STEP))
    _open_interest(database, lower_bound_ms=0, open_times_ms=(0, STEP))

    result = _bounds(database).execute(OI_STREAM)

    assert result.earliest_committed_open_time_ms == 0
    assert result.latest_committed_open_time_ms == STEP


def test_track_without_bounds_gives_no_bounds(tmp_path: Path) -> None:
    unresolved = _database(tmp_path / "unresolved")
    _candles(unresolved, OI_STREAM, (0, STEP))
    _open_interest(unresolved, lower_bound_ms=None, open_times_ms=(0, STEP))

    no_points = _database(tmp_path / "no_points")
    _candles(no_points, OI_STREAM, (0, STEP))
    _open_interest(no_points, lower_bound_ms=0, open_times_ms=())

    no_candles = _database(tmp_path / "no_candles")
    _open_interest(no_candles, lower_bound_ms=0, open_times_ms=(0, STEP))

    for database in (unresolved, no_points, no_candles):
        result = _bounds(database).execute(OI_STREAM)
        assert result.earliest_committed_open_time_ms is None
        assert result.latest_committed_open_time_ms is None


def test_empty_intersection_gives_no_bounds(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, (0, STEP))
    _open_interest(database, lower_bound_ms=3 * STEP, open_times_ms=(3 * STEP,))

    result = _bounds(database).execute(OI_STREAM)

    assert result.earliest_committed_open_time_ms is None
    assert result.latest_committed_open_time_ms is None


def test_bounds_of_degraded_flagged_stream_are_read_only(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, (0, STEP, 2 * STEP))
    _open_interest(database, lower_bound_ms=STEP, open_times_ms=(STEP, 2 * STEP))
    with SqliteUnitOfWork(database) as unit_of_work:
        state = unit_of_work.get_stream_state(OI_STREAM)
        unit_of_work.save_stream_state(replace(state, state=StreamLifecycleState.DEGRADED))
        unit_of_work.commit()
    with SqliteUnitOfWork(database) as unit_of_work:
        state_before = unit_of_work.get_stream_state(OI_STREAM)
        progress_before = unit_of_work.get_track_progress(OI_STREAM, OPEN_INTEREST_TRACK)

    result = _bounds(database).execute(OI_STREAM)

    assert result.state == "degraded"
    assert (result.earliest_committed_open_time_ms, result.latest_committed_open_time_ms) == (
        STEP,
        2 * STEP,
    )
    with SqliteUnitOfWork(database) as unit_of_work:
        assert unit_of_work.get_stream_state(OI_STREAM) == state_before
        assert unit_of_work.get_track_progress(OI_STREAM, OPEN_INTEREST_TRACK) == progress_before
