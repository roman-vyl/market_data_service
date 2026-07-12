from __future__ import annotations

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
from market_data_service.application.smoke_audit_continuity import (
    run_smoke_audit_continuity_workflow,
)
from market_data_service.domain import (
    InstrumentKey,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    TimeWindow,
)


@dataclass
class FakeClock:
    value: int = 1_000_000

    def now_ms(self) -> int:
        current = self.value
        self.value += 1
        return current


class FakeHistoricalSource:
    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        return tuple(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open="100",
                high="102",
                low="99",
                close="101",
                volume="1.5",
                confirmed=True,
                observed_at_ms=observed_at_ms,
                source=ObservationSource.BYBIT_REST,
            )
            for open_time_ms in range(window.start_ms, window.end_ms, 60_000)
        )


def test_smoke_audit_continuity_workflow_passes_on_complete_backfill(
    tmp_path: Path,
) -> None:
    database = tmp_path / "smoke.sqlite3"
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    clock = FakeClock()
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=clock.now_ms())

    def unit_of_work_factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    importer = ImportHistoricalWindow(FakeHistoricalSource(), unit_of_work_factory, clock)
    backfill = BackfillStreamHistory(
        importer,
        unit_of_work_factory,
        clock,
        max_candles_per_window=1000,
    )
    result = run_smoke_audit_continuity_workflow(
        stream=stream,
        window=TimeWindow(0, 180_000),
        backfill=backfill,
        auditor=AuditStreamContinuity(unit_of_work_factory),
    )

    assert result.ok
    assert result.backfill_committed == 3
    assert result.audit.candle_count == 3
    assert result.audit.is_continuous is True
    assert result.audit.gaps == ()
