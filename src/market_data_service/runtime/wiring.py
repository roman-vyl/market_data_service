"""Construct existing historical and realtime components for the runtime."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitOpenInterestSource, BybitRestCandleSource
from market_data_service.adapters.bybit.open_interest_source import MAX_POINTS_PER_WINDOW
from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.adapters.sqlite.consumer_candle_reader import (
    SqliteConsumerCandleReader,
)
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    open_interest_open_times,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.consumer_read import (
    GetCandleRange,
    GetHistoricalCandleRange,
)
from market_data_service.application.history_planning import AuditStreamRange, GetStreamBounds
from market_data_service.application.import_open_interest import ImportOpenInterestWindow
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.lower_bound_probe import OpenInterestLowerBoundProbe
from market_data_service.application.open_interest_track_state import OpenInterestTrackState
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.recovery import RealtimeRecoveryCoordinator
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.config import ValidatedMarketConfig
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


@dataclass(slots=True)
class RuntimeWiring:
    database: Path
    config: ValidatedMarketConfig
    rest_source: BybitRestCandleSource
    clock: SystemClock

    @classmethod
    def build(
        cls,
        database: Path,
        config: ValidatedMarketConfig,
        *,
        rest_base_url: str,
    ) -> RuntimeWiring:
        return cls(
            database=database,
            config=config,
            rest_source=BybitRestCandleSource(
                exchange_symbols=config.exchange_symbols,
                category=config.source.category,
                base_url=rest_base_url,
            ),
            clock=SystemClock(),
        )

    def unit_of_work(self) -> SqliteUnitOfWork:
        return SqliteUnitOfWork(self.database)

    def consumer_read(self) -> GetCandleRange:
        return GetCandleRange(
            self.config,
            SqliteConsumerCandleReader(self.database),
        )

    def historical_read(self) -> GetHistoricalCandleRange:
        return GetHistoricalCandleRange(
            self.config,
            SqliteConsumerCandleReader(self.database),
        )

    def stream_bounds(self) -> GetStreamBounds:
        return GetStreamBounds(self.unit_of_work)

    def continuity_audit_read(self) -> AuditStreamRange:
        return AuditStreamRange(
            self.auditor(),
            self.unit_of_work,
            SqliteConsumerCandleReader(self.database),
        )

    def backfill(self) -> BackfillStreamHistory:
        importer = ImportHistoricalWindow(self.rest_source, self.unit_of_work, self.clock)
        return BackfillStreamHistory(importer, self.unit_of_work, self.clock)

    def lower_bound(self) -> ResolveHistoricalLowerBound:
        return ResolveHistoricalLowerBound(
            self.rest_source,
            self.rest_source,
            self.unit_of_work,
            self.clock,
        )

    def auditor(self) -> AuditStreamContinuity:
        return AuditStreamContinuity(self.unit_of_work)

    def repair(self) -> RepairStreamGaps:
        auditor = self.auditor()
        importer = ImportHistoricalWindow(self.rest_source, self.unit_of_work, self.clock)
        return RepairStreamGaps(
            auditor,
            importer,
            self.unit_of_work,
            self.clock,
        )

    def recovery(self) -> RealtimeRecoveryCoordinator:
        return RealtimeRecoveryCoordinator(
            repair=self.repair(),
            unit_of_work_factory=self.unit_of_work,
            now_ms=self.clock.now_ms,
        )

    def candle_handler(self) -> RealtimeCandleHandler:
        return RealtimeCandleHandler(
            IngestObservedCandle(self.unit_of_work),
            self.clock.now_ms,
        )

    def lifecycle(self) -> RuntimeLifecycleRecorder:
        return RuntimeLifecycleRecorder(self.unit_of_work, self.clock.now_ms)

    def open_interest_state(self) -> OpenInterestTrackState:
        return OpenInterestTrackState(self.unit_of_work, self.clock)

    def open_interest_lower_bound(self) -> ResolveHistoricalLowerBound:
        probe = OpenInterestLowerBoundProbe(
            BybitOpenInterestSource(self.rest_source),
            max_points_per_probe=MAX_POINTS_PER_WINDOW,
        )
        return ResolveHistoricalLowerBound(
            self.rest_source,
            self.rest_source,
            self.unit_of_work,
            self.clock,
            probe=probe,
            progress=self.open_interest_state(),
        )

    def open_interest_importer(self) -> ImportOpenInterestWindow:
        source = BybitOpenInterestSource(self.rest_source)
        return ImportOpenInterestWindow(source, self.unit_of_work, self.clock)

    def open_interest_repair(self) -> RepairStreamGaps:
        return RepairStreamGaps(
            AuditStreamContinuity(self.unit_of_work, open_interest_open_times),
            self.open_interest_importer(),
            self.unit_of_work,
            self.clock,
            max_candles_per_window=MAX_POINTS_PER_WINDOW,
            state=self.open_interest_state(),
        )
