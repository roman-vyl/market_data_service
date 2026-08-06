"""Real end-to-end proof: canonical commit precedes notifier delivery.

Uses the real `IngestObservedCandle` -> `RealtimeCandleHandler` ->
`RuntimeRealtimeCoordinator.on_outcome` -> `CommittedBarNotificationWorker`
chain against a real (temp-file) SQLite database, recording exactly when
the canonical unit-of-work commits and exactly when the notifier's `send()`
is invoked, then asserts the observed order — not merely that the code is
structured to imply it.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import RealtimeIngestionClassification
from market_data_service.application.realtime.recovery_types import RealtimeRecoveryResult
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.status import RuntimeStatusStore


class _OrderRecordingUnitOfWork(SqliteUnitOfWork):
    """Real SqliteUnitOfWork; records "commit" into a shared order list."""

    def __init__(self, database_path: Path, order: list[str], lock: threading.Lock) -> None:
        super().__init__(database_path)
        self._order = order
        self._lock = lock

    def commit(self) -> None:
        super().commit()
        with self._lock:
            self._order.append("commit")


class _OrderRecordingNotifier:
    """Records "send" into the shared order list when notified."""

    def __init__(self, order: list[str], lock: threading.Lock) -> None:
        self._order = order
        self._lock = lock
        self.calls: list[CommittedBarNotification] = []
        self.sent = threading.Event()

    def send(self, notification: CommittedBarNotification) -> None:
        self.calls.append(notification)
        with self._lock:
            self._order.append("send")
        self.sent.set()


class _NoRecovery:
    async def execute(self, request: object) -> RealtimeRecoveryResult:
        raise AssertionError("recovery must not run in this scenario")


def _confirmed_event(stream: StreamKey) -> CandleObserved:
    return CandleObserved(
        stream,
        ObservedCandle(
            stream=stream,
            open_time_ms=0,
            close_time_ms=59_999,
            open="100",
            high="102",
            low="99",
            close="101",
            volume="10",
            confirmed=True,
            observed_at_ms=60_000,
            source=ObservationSource.BYBIT_WEBSOCKET,
        ),
    )


def test_canonical_commit_precedes_notifier_send(tmp_path: Path) -> None:
    asyncio.run(_commit_precedes_send_scenario(tmp_path))


async def _commit_precedes_send_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)

    order: list[str] = []
    order_lock = threading.Lock()

    def unit_of_work_factory() -> _OrderRecordingUnitOfWork:
        return _OrderRecordingUnitOfWork(path, order, order_lock)

    ingestion = IngestObservedCandle(unit_of_work_factory)
    handler = RealtimeCandleHandler(ingestion, now_ms=lambda: 10)

    # Real ingestion, driven exactly as the live WebSocket receive loop
    # would drive it: candle observed -> handler.handle(...).
    outcome = handler.handle(_confirmed_event(stream))
    assert outcome is not None
    assert outcome.classification is RealtimeIngestionClassification.COMMITTED
    # The commit already happened synchronously inside handler.handle(...),
    # strictly before any notification exists.
    assert order == ["commit"]

    notifier = _OrderRecordingNotifier(order, order_lock)
    notifier_worker = CommittedBarNotificationWorker(notifier, capacity=4)
    # A separate, non-recording UoW factory for lifecycle bookkeeping, so
    # stream-lifecycle-state commits (unrelated to the canonical candle
    # commit under test) cannot pollute `order`.
    lifecycle = RuntimeLifecycleRecorder(lambda: SqliteUnitOfWork(path), lambda: 10)
    runtime = RuntimeRealtimeCoordinator(
        streams=(stream,),
        connector=_IdleConnector(),  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor((stream,), {"kline.1.BTCUSDT": stream}, lambda: 10),
        recovery=_NoRecovery(),  # type: ignore[arg-type]
        lifecycle=lifecycle,
        status=RuntimeStatusStore((stream,)),
        admission=RealtimeAdmissionGate((stream,)),
        operation_gate=asyncio.Lock(),
        now_ms=lambda: 10,
        max_recovery_windows=1,
        notifier_worker=notifier_worker,
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))

    # Exactly the same real outcome object the connector's receive loop
    # would have handed to on_outcome, driven through the real gate/worker.
    await runtime.on_outcome(outcome)
    await asyncio.to_thread(notifier.sent.wait, 5)

    stop.set()
    await runner

    assert order == ["commit", "send"]
    assert notifier.calls == [
        CommittedBarNotification(instrument="BTCUSDT.P", timeframe="1m", open_time_ms=0)
    ]


class _IdleConnector:
    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()
