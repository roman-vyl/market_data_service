"""Prefetch on a real SQLite file: no write lock during REST, and the full publication path."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from tests.test_open_interest_stream_track import (
    ListNotifier,
    Recorder,
    _commit,
    _factory,
    _publication,
    _supervisor,
)
from tests.test_open_interest_track import (
    STREAM,
    FakeClock,
    FakeOpenInterestSource,
    _bar,
    _prepare,
)

from market_data_service.application.import_open_interest import ImportOpenInterestWindow
from market_data_service.application.open_interest_live import AppendLiveOpenInterest
from market_data_service.domain.windows import TimeWindow
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
    bar_notification,
)
from market_data_service.runtime.open_interest_prefetch import OpenInterestPrefetch


class LockProbeSource(FakeOpenInterestSource):
    """While the REST call runs, another connection must be able to start a write."""

    def __init__(self, points: dict[int, str], path: Path) -> None:
        super().__init__(points)
        self._path = path
        self.write_lock_free_during_fetch: list[bool] = []

    def fetch_open_interest(self, stream, window, *, observed_at_ms):  # type: ignore[no-untyped-def]
        probe = sqlite3.connect(self._path, timeout=0)
        try:
            probe.execute("BEGIN IMMEDIATE")
            self.write_lock_free_during_fetch.append(True)
            probe.rollback()
        except sqlite3.OperationalError:
            self.write_lock_free_during_fetch.append(False)
        finally:
            probe.close()
        return super().fetch_open_interest(stream, window, observed_at_ms=observed_at_ms)


def _append(path: Path, source: FakeOpenInterestSource) -> AppendLiveOpenInterest:
    factory = _factory(path)
    return AppendLiveOpenInterest(
        ImportOpenInterestWindow(source, factory, FakeClock(_bar(5) + 1_000)), factory
    )


def test_no_write_lock_is_held_while_the_rest_request_runs(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 5))
    source = LockProbeSource({_bar(5): "12"}, path)
    assert _append(path, source).execute(STREAM, _bar(5))
    assert source.write_lock_free_during_fetch == [True]
    # the request is the only slow step and it happens before the short write transaction
    assert source.calls == [TimeWindow(_bar(5), _bar(6))]


def test_prefetched_point_is_published_once_without_rest_and_moves_the_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 5))
    source = FakeOpenInterestSource({_bar(5): "12"})
    append = _append(path, source).execute
    now = [_bar(5) + 20_000]
    supervisor = _supervisor(now, candle=_bar(4), open_interest=_bar(4))
    notifier = ListNotifier()

    async def scenario() -> None:
        prefetch = OpenInterestPrefetch(
            streams=(STREAM,),
            append=append,
            facts=supervisor.facts,
            admitted=lambda _s: True,
            now_ms=lambda: now[0],
            delay_seconds=20,
            retry_seconds=15,
            concurrency=4,
        )
        prefetch.tick()
        await prefetch.settle()
        # stored during the bar; the prefetch itself never moves the frontier
        assert source.calls == [TimeWindow(_bar(5), _bar(6))]
        assert supervisor.facts(STREAM).last_open_interest_open_time_ms == _bar(4)

        now[0] = _bar(6) + 500  # bar closed, confirmed candle arrives
        _commit(supervisor, STREAM, _bar(5))
        publication = _publication(supervisor, append, Recorder())
        worker = CommittedBarNotificationWorker(notifier, 4, prepare=publication.prepare)
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        await worker.enqueue(bar_notification(STREAM, _bar(5)))
        for _ in range(200):
            if notifier.sent:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        stop.set()
        await task

    asyncio.run(scenario())
    assert len(source.calls) == 1  # publication made no REST request
    assert [n.open_time_ms for n in notifier.sent] == [_bar(5)]
    assert supervisor.facts(STREAM).last_open_interest_open_time_ms == _bar(5)
