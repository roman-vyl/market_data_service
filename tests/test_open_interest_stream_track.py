from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from pathlib import Path

from tests.test_open_interest_track import (
    STREAM,
    FakeClock,
    FakeOpenInterestSource,
    _bar,
    _prepare,
)

from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.import_open_interest import ImportOpenInterestWindow
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.open_interest_live import AppendLiveOpenInterest
from market_data_service.application.realtime.events import (
    RecoveryReason,
    RecoveryRequired,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.application.realtime.recovery import (
    RealtimeRecoveryCoordinator,
    RealtimeRecoveryRequest,
    RecoveryClassification,
)
from market_data_service.application.realtime.recovery_open_interest import (
    OpenInterestRecoveryTrack,
)
from market_data_service.application.realtime.supervisor import (
    RealtimeSupervisor,
    StalePolicy,
)
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
    RepairWindowResult,
)
from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.continuity import ContinuityReport
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
    bar_notification,
)
from market_data_service.runtime.open_interest_publication import OpenInterestPublication
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.startup_types import (
    ReconciliationWindow,
    StartupClassification,
    StartupStreamOutcome,
)

TOPIC = "kline.5.BTCUSDT"
PLAIN = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")


def _supervisor(
    now: list[int], *, candle: int | None, open_interest: int | None
) -> RealtimeSupervisor:
    supervisor = RealtimeSupervisor(
        (STREAM, PLAIN),
        {TOPIC: STREAM, "kline.5.ETHUSDT": PLAIN},
        lambda: now[0],
        stale_policy=StalePolicy(grace_ms=0),
        initial_latest_open_time_ms={STREAM: candle, PLAIN: candle},
        initial_open_interest_open_time_ms={STREAM: open_interest},
    )
    supervisor.observe_event(SubscriptionConfirmed((TOPIC, "kline.5.ETHUSDT"), observed_at_ms=1))
    supervisor.record_recovery_result(STREAM, restored=True)
    supervisor.record_recovery_result(PLAIN, restored=True)
    return supervisor


def _commit(supervisor: RealtimeSupervisor, stream: StreamKey, open_time_ms: int) -> None:
    supervisor.observe_outcome(
        RealtimeIngestionOutcome(stream, open_time_ms, RealtimeIngestionClassification.COMMITTED)
    )


def test_open_interest_frontier_follows_the_candle_rule() -> None:
    now = [_bar(5) + 1]
    supervisor = _supervisor(now, candle=_bar(3), open_interest=_bar(3))
    _commit(supervisor, STREAM, _bar(4))

    facts = supervisor.facts(STREAM)
    assert facts.open_interest_required
    assert facts.continuity_frontier_ms == _bar(3)
    assert not facts.published_through(_bar(4))

    assert supervisor.observe_open_interest(STREAM, _bar(4)) == ()
    assert supervisor.facts(STREAM).published_through(_bar(4))

    signals = supervisor.observe_open_interest(STREAM, _bar(6))
    assert signals[0].reason is RecoveryReason.SEQUENCE_DISCONTINUITY
    assert signals[0].suspected_start_time_ms == _bar(5)
    assert not supervisor.facts(STREAM).data_ready


def test_stale_uses_lower_frontier_and_plain_streams_are_unchanged() -> None:
    now = [_bar(5) + 1]
    supervisor = _supervisor(now, candle=_bar(3), open_interest=_bar(3))
    _commit(supervisor, STREAM, _bar(4))
    _commit(supervisor, PLAIN, _bar(4))

    signals = supervisor.detect_stale()
    assert [(s.stream, s.reason, s.suspected_start_time_ms) for s in signals] == [
        (STREAM, RecoveryReason.STALE, _bar(4))
    ]
    plain = supervisor.facts(PLAIN)
    assert not plain.open_interest_required
    assert plain.continuity_frontier_ms == _bar(4)
    assert plain.published_through(_bar(4))


def test_restored_episode_advances_both_frontiers() -> None:
    now = [_bar(9) + 1]
    supervisor = _supervisor(now, candle=_bar(3), open_interest=_bar(1))
    supervisor.record_recovery_result(STREAM, restored=True, restored_through_open_time_ms=_bar(8))
    facts = supervisor.facts(STREAM)
    assert facts.last_successful_open_time_ms == _bar(8)
    assert facts.last_open_interest_open_time_ms == _bar(8)
    assert supervisor.facts(PLAIN).last_open_interest_open_time_ms is None


def test_missing_open_interest_frontier_is_not_continuity() -> None:
    now = [_bar(5) + 1]
    supervisor = _supervisor(now, candle=_bar(4), open_interest=None)
    facts = supervisor.facts(STREAM)
    assert facts.continuity_frontier_ms is None
    assert not facts.published_through(_bar(4))


class Recorder:
    def __init__(self) -> None:
        self.signals: list[RecoveryRequired] = []

    async def __call__(self, signal: RecoveryRequired) -> None:
        self.signals.append(signal)


def _publication(
    supervisor: RealtimeSupervisor, append, recorder: Recorder
) -> OpenInterestPublication:  # type: ignore[no-untyped-def]
    return OpenInterestPublication(
        streams=(STREAM,),
        append=append,
        supervisor=supervisor,
        request_recovery=recorder,
        logger=logging.getLogger("test.oi_publication"),
    )


def test_publication_sends_only_data_ready_bars() -> None:
    now = [_bar(5) + 1]
    supervisor = _supervisor(now, candle=_bar(3), open_interest=_bar(3))
    recorder = Recorder()
    calls: list[tuple[StreamKey, int]] = []

    def append(stream: StreamKey, open_time_ms: int) -> bool:
        calls.append((stream, open_time_ms))
        return True

    publication = _publication(supervisor, append, recorder)
    assert asyncio.run(publication.prepare(bar_notification(PLAIN, _bar(4))))
    assert calls == []

    _commit(supervisor, STREAM, _bar(4))
    assert asyncio.run(publication.prepare(bar_notification(STREAM, _bar(4))))
    assert calls == [(STREAM, _bar(4))]

    _commit(supervisor, STREAM, _bar(6))
    assert not asyncio.run(publication.prepare(bar_notification(STREAM, _bar(6))))
    assert recorder.signals and recorder.signals[-1].suspected_start_time_ms == _bar(5)


def test_publication_holds_bar_when_open_interest_is_missing() -> None:
    now = [_bar(5) + 1]
    supervisor = _supervisor(now, candle=_bar(3), open_interest=_bar(3))
    recorder = Recorder()

    def failing(stream: StreamKey, open_time_ms: int) -> bool:
        raise TimeoutError("slow")

    _commit(supervisor, STREAM, _bar(4))
    assert not asyncio.run(
        _publication(supervisor, failing, recorder).prepare(bar_notification(STREAM, _bar(4)))
    )
    assert not asyncio.run(
        _publication(supervisor, lambda s, t: False, recorder).prepare(
            bar_notification(STREAM, _bar(4))
        )
    )
    assert supervisor.facts(STREAM).last_open_interest_open_time_ms == _bar(3)
    assert recorder.signals == []


class ListNotifier:
    def __init__(self) -> None:
        self.sent: list[CommittedBarNotification] = []

    def send(self, notification: CommittedBarNotification) -> None:
        self.sent.append(notification)


def test_worker_skips_send_when_prepare_holds_the_bar() -> None:
    notifier = ListNotifier()

    async def prepare(notification: CommittedBarNotification) -> bool:
        return notification.open_time_ms != _bar(1)

    async def scenario() -> None:
        worker = CommittedBarNotificationWorker(notifier, 4, prepare=prepare)
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        for index in (0, 1, 2):
            await worker.enqueue(bar_notification(STREAM, _bar(index)))
        for _ in range(100):
            if len(notifier.sent) == 2:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await task

    asyncio.run(scenario())
    assert [item.open_time_ms for item in notifier.sent] == [_bar(0), _bar(2)]


def _factory(path: Path):  # type: ignore[no-untyped-def]
    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(path)

    return factory


def test_live_append_fetches_only_a_missing_point(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 5))
    source = FakeOpenInterestSource({_bar(3): "10", _bar(4): "11"})
    factory = _factory(path)
    append = AppendLiveOpenInterest(
        ImportOpenInterestWindow(source, factory, FakeClock(_bar(5))), factory
    )

    assert append.execute(STREAM, _bar(4))
    assert source.calls == [TimeWindow(_bar(4), _bar(5))]
    assert append.execute(STREAM, _bar(4))
    assert len(source.calls) == 1
    assert not append.execute(STREAM, _bar(2))


class NoCandleSource:
    def fetch_closed_candles(
        self, stream: StreamKey, window: TimeWindow, *, observed_at_ms: int
    ) -> tuple[ObservedCandle, ...]:
        return ()


class FakeOpenInterestRepair:
    def __init__(self, status: RepairStatus, *, disposition: str | None = None) -> None:
        self.status = status
        self.disposition = disposition
        self.requests: list[RepairStreamGapsRequest] = []

    def execute(self, request: RepairStreamGapsRequest) -> RepairStreamGapsResult:
        self.requests.append(request)
        window = TimeWindow(request.start_time_ms, request.end_time_ms)
        report = ContinuityReport(STREAM, window.start_ms, window.end_ms, 0, True, ())
        return RepairStreamGapsResult(
            stream=request.stream,
            requested_window=window,
            status=self.status,
            pre_repair_audit=report,
            post_repair_audit=report,
            attempted_windows=1,
            completed_windows=1,
            window_results=(RepairWindowResult(window, 3, 3, 0, 0, 0, 0),),
            error_code=None if self.status is not RepairStatus.FAILED else "oi_failed",
            failure_disposition=self.disposition,  # type: ignore[arg-type]
        )


def _recover(path: Path, oi_repair: FakeOpenInterestRepair):  # type: ignore[no-untyped-def]
    factory = _factory(path)
    with factory() as unit_of_work:
        snapshot = unit_of_work.get_stream_state(STREAM)
        unit_of_work.save_stream_state(replace(snapshot, latest_committed_open_time_ms=_bar(9)))
        unit_of_work.commit()
    clock = FakeClock(_bar(10) + 1_000)
    candle_repair = RepairStreamGaps(
        AuditStreamContinuity(factory),
        ImportHistoricalWindow(NoCandleSource(), factory, clock),
        factory,
        clock,
        max_candles_per_window=10,
    )
    coordinator = RealtimeRecoveryCoordinator(
        repair=candle_repair,
        unit_of_work_factory=factory,
        now_ms=clock.now_ms,
        open_interest=OpenInterestRecoveryTrack(
            streams=(STREAM,), repair=oi_repair, latest_open_time_ms=lambda s: _bar(4)
        ),
    )
    signal = RecoveryRequired(
        STREAM,
        RecoveryReason.SEQUENCE_DISCONTINUITY,
        detected_at_ms=_bar(10),
        suspected_start_time_ms=_bar(8),
    )
    return asyncio.run(coordinator.execute(RealtimeRecoveryRequest(signal, max_windows=2)))


def test_recovery_restores_only_when_open_interest_is_continuous(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 10))
    complete = FakeOpenInterestRepair(RepairStatus.COMPLETE)
    result = _recover(path, complete)
    assert result.classification is RecoveryClassification.RESTORED
    assert result.recovery_window.start_ms == _bar(5)
    request = complete.requests[0]
    assert (request.start_time_ms, request.end_time_ms) == (_bar(5), _bar(10))
    assert result.committed_bars == 3


def test_recovery_is_not_restored_while_open_interest_has_gaps(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 10))
    result = _recover(path, FakeOpenInterestRepair(RepairStatus.INCOMPLETE))
    assert result.classification is RecoveryClassification.INCOMPLETE
    assert result.error_code == "post_recovery_open_interest_incomplete"

    path2 = tmp_path / "second.sqlite3"
    _prepare(path2, candle_bars=range(0, 10))
    failed = _recover(path2, FakeOpenInterestRepair(RepairStatus.FAILED, disposition="recoverable"))
    assert failed.classification is RecoveryClassification.RECOVERABLE_FAILURE
    assert failed.error_code == "oi_failed"


class FakeLifecycle:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def prepare_for_bootstrap(self, stream: StreamKey) -> None:
        self.calls.append("prepare")

    def mark_auditing(self, stream: StreamKey) -> None:
        self.calls.append("auditing")

    def mark_connecting(self, stream: StreamKey) -> None:
        self.calls.append("connecting")


class FakeOpenInterestPass:
    def __init__(self, classification: StartupClassification) -> None:
        self.classification = classification
        self.calls = 0

    def execute_stream(
        self, stream: StreamKey, window: ReconciliationWindow | None = None
    ) -> StartupStreamOutcome:
        self.calls += 1
        return StartupStreamOutcome(stream, self.classification, error_code="oi_pass")


def _startup(oi: FakeOpenInterestPass, stream: StreamKey):  # type: ignore[no-untyped-def]
    lifecycle = FakeLifecycle()
    reconciler = HistoricalStreamReconciler(
        lower_bound=object(),  # type: ignore[arg-type]
        repair=FakeOpenInterestRepair(RepairStatus.COMPLETE),  # type: ignore[arg-type]
        lifecycle=lifecycle,  # type: ignore[arg-type]
        now_ms=lambda: _bar(10),
        discovery_windows_per_pass=1,
        repair_windows_per_pass=1,
        open_interest=oi,
        open_interest_streams=frozenset({STREAM}),
    )
    window = ReconciliationWindow(_bar(0), _bar(10))
    return reconciler.execute(stream, window), lifecycle, window


def test_startup_hands_off_only_after_open_interest_is_proven() -> None:
    held, lifecycle, window = _startup(
        FakeOpenInterestPass(StartupClassification.INCOMPLETE), STREAM
    )
    assert held.classification is StartupClassification.INCOMPLETE
    assert held.window == window
    assert "connecting" not in lifecycle.calls

    done, lifecycle, _ = _startup(FakeOpenInterestPass(StartupClassification.CONNECTING), STREAM)
    assert done.classification is StartupClassification.CONNECTING
    assert lifecycle.calls[-1] == "connecting"

    plain_pass = FakeOpenInterestPass(StartupClassification.INCOMPLETE)
    plain, _, _ = _startup(plain_pass, PLAIN)
    assert plain.classification is StartupClassification.CONNECTING
    assert plain_pass.calls == 0
