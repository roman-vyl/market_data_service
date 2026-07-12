from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.sqlite import SqliteUnitOfWork, initialize_database, register_stream
from market_data_service.adapters.sqlite.connection import open_connection
from market_data_service.application.audit_continuity import AuditStreamContinuity, AuditStreamContinuityRequest
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import RepairStatus, RepairStreamGapsRequest
from market_data_service.domain import InstrumentKey, StreamKey, TimeWindow
from market_data_service.domain.stream_state import StreamLifecycleState

from tests.fake_bybit_api import FakeBybitApi, FakeBybitState


@dataclass
class Clock:
    value: int = 10_000_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


def _wire(path: Path, base_url: str):
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    initialize_database(path)
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock()
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        base_url=base_url,
    )
    factory = lambda: SqliteUnitOfWork(path)
    importer = ImportHistoricalWindow(source, factory, clock)
    backfill = BackfillStreamHistory(importer, factory, clock, max_candles_per_window=3)
    auditor = AuditStreamContinuity(factory)
    repair = RepairStreamGaps(auditor, importer, factory, clock, max_candles_per_window=2)
    return stream, factory, importer, backfill, auditor, repair


def _audit(auditor, stream, end_ms):
    return auditor.execute(AuditStreamContinuityRequest(stream, 0, end_ms))


def test_fake_api_pipeline_matrix(tmp_path: Path) -> None:
    state = FakeBybitState()
    state.seed_symbol("BTCUSDT", start_ms=0, count=8, base=100)
    with FakeBybitApi(state) as api:
        path = tmp_path / "market.sqlite3"
        stream, factory, importer, backfill, auditor, repair = _wire(path, api.base_url)

        first = backfill.execute(BackfillStreamRequest(stream, 0, 480_000, max_windows=3))
        assert first.completed_windows == 3
        assert sum(item.committed for item in first.window_results) == 8
        assert _audit(auditor, stream, 480_000).is_continuous

        replay = backfill.execute(BackfillStreamRequest(
            stream, 0, 480_000, max_windows=3, resume_from_latest_committed=False
        ))
        assert sum(item.duplicates for item in replay.window_results) == 8

        state.remove("BTCUSDT", 120_000, 300_000)
        connection = open_connection(path)
        try:
            connection.execute("DELETE FROM candles WHERE open_time_ms IN (?, ?)", (120_000, 300_000))
            connection.commit()
        finally:
            connection.close()
        gaps = _audit(auditor, stream, 480_000)
        assert [(gap.start_ms, gap.end_ms) for gap in gaps.gaps] == [
            (120_000, 180_000), (300_000, 360_000)
        ]

        state.seed_symbol("BTCUSDT", start_ms=0, count=8, base=100)
        repaired = repair.execute(RepairStreamGapsRequest(stream, 0, 480_000, max_windows=4))
        assert repaired.status is RepairStatus.COMPLETE
        assert repaired.post_repair_audit and repaired.post_repair_audit.is_continuous

        state.mutate_close("BTCUSDT", 180_000, "105")
        corrected = importer.execute(stream, TimeWindow(180_000, 240_000))
        assert corrected.corrected == 1
        with factory() as uow:
            candle = uow.get_candle(stream, 180_000)
        assert candle is not None and candle.ohlcv_text[3] == "105"

        state.transient_kline_failures = 1
        failed = backfill.execute(BackfillStreamRequest(
            stream, 0, 60_000, max_windows=1, resume_from_latest_committed=False
        ))
        assert failed.failure_disposition == "recoverable"
        with factory() as uow:
            snapshot = uow.get_stream_state(stream)
        assert snapshot.state is StreamLifecycleState.DEGRADED

        recovered = backfill.execute(BackfillStreamRequest(
            stream, 0, 60_000, max_windows=1, resume_from_latest_committed=False
        ))
        assert recovered.error_code is None
        assert recovered.window_results[0].duplicates == 1
