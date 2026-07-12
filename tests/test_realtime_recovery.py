from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.realtime.events import (
    RecoveryReason,
    RecoveryRequired,
)
from market_data_service.application.realtime.recovery import (
    RealtimeRecoveryCoordinator,
    RealtimeRecoveryRequest,
    RecoveryClassification,
)
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.windows import TimeWindow


@dataclass
class Clock:
    value: int

    def now_ms(self) -> int:
        return self.value


class SequencedHistoricalSource:
    def __init__(self, stream: StreamKey, *, omit_first: set[int] | None = None) -> None:
        self.stream = stream
        self.omit_first = omit_first or set()
        self.calls = 0

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        assert stream == self.stream
        self.calls += 1
        omitted = self.omit_first if self.calls == 1 else set()
        rows = []
        for open_time in range(window.start_ms, window.end_ms, 60_000):
            if open_time in omitted:
                continue
            rows.append(
                ObservedCandle(
                    stream=stream,
                    open_time_ms=open_time,
                    close_time_ms=open_time + 59_999,
                    open="100",
                    high="102",
                    low="99",
                    close="101",
                    volume="10",
                    confirmed=True,
                    observed_at_ms=observed_at_ms,
                    source=ObservationSource.BYBIT_REST,
                )
            )
        return tuple(rows)


def _wire(database: Path, source: SequencedHistoricalSource, clock: Clock):
    def uow_factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    importer = ImportHistoricalWindow(source, uow_factory, clock)
    backfill = BackfillStreamHistory(
        importer, uow_factory, clock, max_candles_per_window=10
    )
    auditor = AuditStreamContinuity(uow_factory)
    repair = RepairStreamGaps(
        auditor, importer, uow_factory, clock, max_candles_per_window=10
    )
    coordinator = RealtimeRecoveryCoordinator(
        backfill=backfill,
        auditor=auditor,
        repair=repair,
        unit_of_work_factory=uow_factory,
        now_ms=clock.now_ms,
    )
    return uow_factory, backfill, coordinator


def test_sequence_hint_recovers_internal_gap_and_requires_post_audit(tmp_path: Path) -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock(300_000)
    source = SequencedHistoricalSource(stream)
    uow_factory, backfill, coordinator = _wire(database, source, clock)

    initial = backfill.execute(
        __import__(
            "market_data_service.application.backfill_types", fromlist=["BackfillStreamRequest"]
        ).BackfillStreamRequest(stream, 0, 180_000, max_windows=1)
    )
    assert initial.reached_end
    # Simulate a later confirmed WebSocket close that advanced durable tail past 180000.
    with uow_factory() as uow:
        from market_data_service.application.ingest import IngestObservedCandle

        IngestObservedCandle.execute_in_unit_of_work(
            uow,
            ObservedCandle(
                stream=stream,
                open_time_ms=240_000,
                close_time_ms=299_999,
                open="100",
                high="102",
                low="99",
                close="101",
                volume="10",
                confirmed=True,
                observed_at_ms=300_000,
                source=ObservationSource.BYBIT_WEBSOCKET,
            ),
            committed_at_ms=300_000,
        )
        uow.commit()

    result = asyncio.run(
        coordinator.execute(
            RealtimeRecoveryRequest(
                RecoveryRequired(
                    stream,
                    RecoveryReason.SEQUENCE_DISCONTINUITY,
                    detected_at_ms=300_000,
                    suspected_start_time_ms=180_000,
                ),
                max_backfill_windows=1,
                max_repair_windows=1,
            )
        )
    )
    assert result.classification is RecoveryClassification.RESTORED
    assert result.recovery_window == TimeWindow(180_000, 300_000)
    assert result.post_audit and result.post_audit.is_continuous
    with uow_factory() as uow:
        assert uow.get_candle(stream, 180_000) is not None


def test_partial_backfill_uses_repair_then_post_audit(tmp_path: Path) -> None:
    stream = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="ETHUSDT", now_ms=1)
    clock = Clock(240_000)
    initial_source = SequencedHistoricalSource(stream)
    _, backfill, _ = _wire(database, initial_source, clock)
    assert backfill.execute(
        __import__(
            "market_data_service.application.backfill_types", fromlist=["BackfillStreamRequest"]
        ).BackfillStreamRequest(stream, 0, 120_000, max_windows=1)
    ).reached_end
    recovery_source = SequencedHistoricalSource(stream, omit_first={120_000})
    _, _, coordinator = _wire(database, recovery_source, clock)

    result = asyncio.run(
        coordinator.execute(
            RealtimeRecoveryRequest(
                RecoveryRequired(
                    stream,
                    RecoveryReason.DISCONNECT,
                    detected_at_ms=240_000,
                ),
                max_backfill_windows=1,
                max_repair_windows=1,
            )
        )
    )
    assert result.classification is RecoveryClassification.RESTORED
    assert result.repair is not None
    assert result.post_audit and result.post_audit.is_continuous


def test_missing_durable_anchor_is_incomplete(tmp_path: Path) -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock(120_000)
    source = SequencedHistoricalSource(stream)
    _, _, coordinator = _wire(database, source, clock)

    result = asyncio.run(
        coordinator.execute(
            RealtimeRecoveryRequest(
                RecoveryRequired(stream, RecoveryReason.STALE, detected_at_ms=120_000),
                max_backfill_windows=1,
                max_repair_windows=1,
            )
        )
    )
    assert result.classification is RecoveryClassification.INCOMPLETE
    assert result.error_code == "missing_durable_recovery_anchor"
