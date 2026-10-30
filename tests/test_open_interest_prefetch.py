"""Open interest prefetch: fetch point T during bar T, never after it closed."""

from __future__ import annotations

import asyncio
import logging
import threading
from types import SimpleNamespace

import pytest

from market_data_service.application.open_interest_live import AppendLiveOpenInterest
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.runtime.open_interest_composition import open_interest_prefetch
from market_data_service.runtime.open_interest_prefetch import OpenInterestPrefetch

STEP = 300_000
T0 = 100 * STEP
STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class Source:
    """Scripted append: per call a bool, or an exception to raise; records calls."""

    def __init__(self, answers: list[bool | Exception]) -> None:
        self._answers = list(answers)
        self.calls: list[tuple[StreamKey, int]] = []

    def __call__(self, stream: StreamKey, open_time_ms: int) -> bool:
        self.calls.append((stream, open_time_ms))
        answer = self._answers.pop(0) if self._answers else False
        if isinstance(answer, Exception):
            raise answer
        return answer


def _facts(pending: bool = False, active: bool = True):
    return lambda _stream: SimpleNamespace(subscription_active=active, recovery_pending=pending)


def _prefetch(
    source,
    clock,
    *,
    streams=(STREAM,),
    facts=None,
    admitted=lambda _s: True,
    concurrency: int = 4,
):
    return OpenInterestPrefetch(
        streams=streams,
        append=source,
        facts=facts or _facts(),
        admitted=admitted,
        now_ms=clock,
        delay_seconds=20,
        retry_seconds=15,
        concurrency=concurrency,
    )


async def _tick(prefetch: OpenInterestPrefetch) -> None:
    prefetch.tick()
    await prefetch.settle()


def test_stored_at_first_attempt_after_the_delay_and_not_requested_again() -> None:
    async def scenario() -> None:
        clock, source = Clock(T0 + 19_000), Source([True])
        prefetch = _prefetch(source, clock)
        await _tick(prefetch)
        assert source.calls == []
        clock.now = T0 + 20_000
        await _tick(prefetch)
        clock.now = T0 + 120_000
        await _tick(prefetch)
        assert source.calls == [(STREAM, T0)]

    asyncio.run(scenario())


def test_not_published_yet_and_failures_are_retried_every_interval_until_stored() -> None:
    async def scenario() -> None:
        clock = Clock(T0 + 20_000)
        source = Source([False, OSError("handshake timed out"), True])
        prefetch = _prefetch(source, clock)
        await _tick(prefetch)
        clock.now = T0 + 34_000
        await _tick(prefetch)
        assert len(source.calls) == 1
        clock.now = T0 + 35_000
        await _tick(prefetch)
        clock.now = T0 + 50_000
        await _tick(prefetch)
        clock.now = T0 + 200_000
        await _tick(prefetch)
        assert len(source.calls) == 3

    asyncio.run(scenario())


def test_never_available_stops_at_close_and_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        clock, source = Clock(T0 + 20_000), Source([])
        prefetch = _prefetch(source, clock)
        for now in range(T0 + 20_000, T0 + STEP, 15_000):
            clock.now = now
            await _tick(prefetch)
        attempts = len(source.calls)
        assert attempts == 19
        clock.now = T0 + STEP
        await _tick(prefetch)
        clock.now = T0 + STEP + 30_000
        await _tick(prefetch)
        assert [call[1] for call in source.calls[attempts:]] == [T0 + STEP]

    with caplog.at_level(logging.INFO, logger="market_data_service.runtime.oi_prefetch"):
        asyncio.run(scenario())
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "attempts=19" in warnings[0].getMessage()


def test_stored_prefetch_logs_seconds_after_open(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="market_data_service.runtime.oi_prefetch"):
        asyncio.run(_tick(_prefetch(Source([True]), Clock(T0 + 21_000))))
    assert "seconds_after_open=21.0 attempts=1" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"facts": _facts(pending=True)},
        {"facts": _facts(active=False)},
        {"admitted": lambda _s: False},
    ],
)
def test_no_attempt_for_ineligible_stream(kwargs) -> None:
    source = Source([True])
    asyncio.run(_tick(_prefetch(source, Clock(T0 + 60_000), **kwargs)))
    assert source.calls == []


def test_concurrency_is_bounded_and_the_loop_stays_free() -> None:
    streams = tuple(StreamKey(InstrumentKey(f"C{i}USDT.P"), "5m") for i in range(10))
    lock, state = threading.Lock(), {"now": 0, "peak": 0}
    release = threading.Event()

    def append(_stream: StreamKey, _open: int) -> bool:
        with lock:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
        release.wait(5)
        with lock:
            state["now"] -= 1
        return True

    async def scenario() -> None:
        prefetch = _prefetch(append, Clock(T0 + 20_000), streams=streams, concurrency=4)
        prefetch.tick()
        for _ in range(20):
            await asyncio.sleep(0.01)
        assert state["peak"] == 4
        release.set()
        await prefetch.settle()

    asyncio.run(scenario())
    assert state["peak"] == 4


def test_attempt_queued_for_a_slot_sends_nothing_after_the_bar_closed() -> None:
    other = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    release, started = threading.Event(), threading.Event()
    calls: list[StreamKey] = []

    def append(stream: StreamKey, _open: int) -> bool:
        calls.append(stream)
        started.set()
        release.wait(5)
        return True

    async def scenario() -> None:
        clock = Clock(T0 + STEP - 1_000)  # 12:04:59
        prefetch = _prefetch(append, clock, streams=(STREAM, other), concurrency=1)
        prefetch.tick()
        while not started.is_set():
            await asyncio.sleep(0.005)
        clock.now = T0 + STEP  # 12:05:00, the second attempt still waits for the slot
        release.set()  # 12:05:01 the slot frees
        await prefetch.settle()

    asyncio.run(scenario())
    assert calls == [STREAM]


def test_attempt_started_before_close_finishes_and_stores() -> None:
    release, started = threading.Event(), threading.Event()
    stored: list[int] = []

    def append(_stream: StreamKey, open_ms: int) -> bool:
        started.set()
        release.wait(5)
        stored.append(open_ms)
        return True

    async def scenario() -> None:
        clock = Clock(T0 + STEP - 1_000)
        prefetch = _prefetch(append, clock)
        prefetch.tick()
        while not started.is_set():
            await asyncio.sleep(0.005)
        clock.now = T0 + STEP + 1_000
        release.set()
        await prefetch.settle()

    asyncio.run(scenario())
    assert stored == [T0]


def test_run_stops_creating_attempts_and_lets_the_inflight_one_finish() -> None:
    release, started = threading.Event(), threading.Event()
    done: list[int] = []

    def append(_stream: StreamKey, open_ms: int) -> bool:
        started.set()
        release.wait(5)
        done.append(open_ms)
        return True

    async def scenario() -> None:
        prefetch = _prefetch(append, Clock(T0 + 20_000))
        stop = asyncio.Event()
        task = asyncio.create_task(prefetch.run(stop))
        while not started.is_set():
            await asyncio.sleep(0.005)
        stop.set()
        release.set()
        await task

    asyncio.run(scenario())
    assert done == [T0]


def test_second_publication_step_finds_the_prefetched_point_without_a_request() -> None:
    class Importer:
        def __init__(self) -> None:
            self.windows = 0

        def execute(self, _stream, _window):
            self.windows += 1

    class Uow:
        def __init__(self, stored: dict[int, object]) -> None:
            self._stored = stored

        def __enter__(self):
            return self

        def __exit__(self, *_exc) -> None:
            return None

        def get_open_interest(self, _stream, open_ms):
            return self._stored.get(open_ms)

    stored: dict[int, object] = {}

    class StoringImporter(Importer):
        def execute(self, stream, window):
            super().execute(stream, window)
            stored[window.start_ms] = object()

    importer = StoringImporter()
    append = AppendLiveOpenInterest(importer, lambda: Uow(stored)).execute
    asyncio.run(_run_once(append))
    assert importer.windows == 1
    assert append(STREAM, T0) is True  # publication step at close
    assert importer.windows == 1


async def _run_once(append) -> None:
    prefetch = _prefetch(append, Clock(T0 + 20_000))
    await _tick(prefetch)


def test_prefetch_never_reports_to_the_supervisor() -> None:
    touched: list[str] = []

    def facts(_stream: StreamKey):
        touched.append("facts")
        return SimpleNamespace(subscription_active=True, recovery_pending=False)

    asyncio.run(_tick(_prefetch(Source([True]), Clock(T0 + 20_000), facts=facts)))
    assert set(touched) == {"facts"}  # read-only access; no observe_* call exists on it


def _wiring(streams):
    return SimpleNamespace(
        config=SimpleNamespace(open_interest_streams=streams),
        clock=SimpleNamespace(now_ms=lambda: T0),
        open_interest_live=lambda _timeout: SimpleNamespace(execute=Source([])),
    )


def _settings(delay: float):
    return SimpleNamespace(
        oi_prefetch_delay_seconds=delay,
        oi_prefetch_retry_seconds=15.0,
        oi_prefetch_concurrency=4,
        oi_live_timeout_seconds=2.0,
    )


def test_delay_zero_or_no_flagged_streams_builds_nothing() -> None:
    supervisor = SimpleNamespace(facts=_facts())
    admission = SimpleNamespace(allows=lambda _s: True)
    assert open_interest_prefetch(_wiring((STREAM,)), _settings(0), supervisor, admission) is None
    assert open_interest_prefetch(_wiring(()), _settings(20), supervisor, admission) is None
    built = open_interest_prefetch(_wiring((STREAM,)), _settings(20), supervisor, admission)
    assert isinstance(built, OpenInterestPrefetch)
