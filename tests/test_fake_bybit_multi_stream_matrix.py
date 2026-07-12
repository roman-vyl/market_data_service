from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitRestCandleSource
from market_data_service.adapters.sqlite import SqliteUnitOfWork, initialize_database, register_stream
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.full_bootstrap import BootstrapFullStreamHistory
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.market_metadata import VerifyConfiguredInstrumentMetadata
from market_data_service.application.multi_stream_backfill import (
    BackfillAllConfiguredStreams,
    MultiStreamBackfillRequest,
)
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import RepairStatus, RepairStreamGapsRequest
from market_data_service.domain import HistoryPolicy, InstrumentCoverage, InstrumentKey, StreamKey
from market_data_service.domain.timeframes import get_timeframe

from tests.fake_bybit_api import FakeBybitApi, FakeBybitState


@dataclass
class FixedClock:
    value: int

    def now_ms(self) -> int:
        return self.value


def _coverage(ticker: str, timeframes: tuple[str, ...]) -> InstrumentCoverage:
    return InstrumentCoverage(
        instrument=InstrumentKey(ticker),
        exchange_symbol=ticker.removesuffix(".P"),
        enabled=True,
        canonical_timeframes=timeframes,
        history_policy=HistoryPolicy.FULL_AVAILABLE,
    )


def _seed(state: FakeBybitState, coverage: InstrumentCoverage, count: int = 6) -> dict[str, int]:
    ends: dict[str, int] = {}
    base = 100 if coverage.instrument.ticker.startswith("BTC") else 500
    for offset, stream in enumerate(coverage.stream_keys):
        timeframe = get_timeframe(stream.timeframe)
        state.seed_stream(
            coverage.exchange_symbol,
            timeframe.bybit_interval,
            start_ms=0,
            count=count,
            step_ms=timeframe.duration_ms,
            base=base + offset * 100,
        )
        ends[stream.canonical_id] = count * timeframe.duration_ms
    return ends


def test_fake_api_multi_symbol_multi_timeframe_orchestration_and_repair(tmp_path: Path) -> None:
    btc = _coverage("BTCUSDT.P", ("1m", "5m", "1h"))
    eth = _coverage("ETHUSDT.P", ("1m", "5m", "1h"))
    state = FakeBybitState()
    end_by_stream = {**_seed(state, btc), **_seed(state, eth)}

    with FakeBybitApi(state) as api:
        database = tmp_path / "market.sqlite3"
        initialize_database(database)
        for coverage in (btc, eth):
            for stream in coverage.stream_keys:
                register_stream(
                    database,
                    stream,
                    exchange_symbol=coverage.exchange_symbol,
                    now_ms=1,
                )

        source = BybitRestCandleSource(
            exchange_symbols={
                btc.instrument.ticker: btc.exchange_symbol,
                eth.instrument.ticker: eth.exchange_symbol,
            },
            base_url=api.base_url,
        )
        verifier = VerifyConfiguredInstrumentMetadata(source, category="linear")
        uow_factory = lambda: SqliteUnitOfWork(database)

        def bootstrap_factory(
            coverage: InstrumentCoverage, stream: StreamKey
        ) -> BootstrapFullStreamHistory:
            clock = FixedClock(end_by_stream[stream.canonical_id])
            importer = ImportHistoricalWindow(source, uow_factory, clock)
            backfill = BackfillStreamHistory(importer, uow_factory, clock)
            lower_bound = ResolveHistoricalLowerBound(source, source, uow_factory, clock)
            return BootstrapFullStreamHistory(lower_bound, backfill, uow_factory, clock)

        result = BackfillAllConfiguredStreams(
            verifier.execute,
            bootstrap_factory,
        ).execute((btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=2))

        expected_order = [
            "BTCUSDT.P:1m",
            "BTCUSDT.P:5m",
            "BTCUSDT.P:1h",
            "ETHUSDT.P:1m",
            "ETHUSDT.P:5m",
            "ETHUSDT.P:1h",
        ]
        assert [outcome.stream.canonical_id for outcome in result.outcomes] == expected_order
        assert result.status == "complete"
        assert all(outcome.result and outcome.result.reached_target for outcome in result.outcomes)

        auditor = AuditStreamContinuity(uow_factory)
        for coverage in (btc, eth):
            for stream in coverage.stream_keys:
                report = auditor.execute(
                    AuditStreamContinuityRequest(
                        stream,
                        0,
                        end_by_stream[stream.canonical_id],
                    )
                )
                assert report.is_continuous
                assert report.candle_count == 6

        kline_pairs = [
            (query["symbol"][0], query["interval"][0])
            for path, query in state.calls
            if path.endswith("/v5/market/kline")
        ]
        for pair in {
            ("BTCUSDT", "1"),
            ("BTCUSDT", "5"),
            ("BTCUSDT", "60"),
            ("ETHUSDT", "1"),
            ("ETHUSDT", "5"),
            ("ETHUSDT", "60"),
        }:
            assert pair in kline_pairs

        deleted = {
            StreamKey(InstrumentKey("BTCUSDT.P"), "5m"): 2 * 5 * 60_000,
            StreamKey(InstrumentKey("ETHUSDT.P"), "1h"): 2 * 60 * 60_000,
        }
        connection = sqlite3.connect(database)
        try:
            for stream, open_time in deleted.items():
                connection.execute(
                    """
                    DELETE FROM candles
                    WHERE stream_id = (
                        SELECT s.id FROM streams s
                        JOIN instruments i ON i.id = s.instrument_id
                        WHERE i.ticker = ? AND s.timeframe = ?
                    ) AND open_time_ms = ?
                    """,
                    (stream.instrument.ticker, stream.timeframe, open_time),
                )
            connection.commit()
        finally:
            connection.close()

        for stream, missing_open in deleted.items():
            end_ms = end_by_stream[stream.canonical_id]
            before = auditor.execute(AuditStreamContinuityRequest(stream, 0, end_ms))
            step_ms = get_timeframe(stream.timeframe).duration_ms
            assert [(gap.start_ms, gap.end_ms) for gap in before.gaps] == [
                (missing_open, missing_open + step_ms)
            ]
            clock = FixedClock(end_ms)
            importer = ImportHistoricalWindow(source, uow_factory, clock)
            repair = RepairStreamGaps(
                auditor,
                importer,
                uow_factory,
                clock,
                max_candles_per_window=2,
            )
            repaired = repair.execute(
                RepairStreamGapsRequest(stream, 0, end_ms, max_windows=1)
            )
            assert repaired.status is RepairStatus.COMPLETE
            assert repaired.post_repair_audit
            assert repaired.post_repair_audit.is_continuous

        untouched = StreamKey(InstrumentKey("BTCUSDT.P"), "1h")
        with uow_factory() as uow:
            assert uow.get_candle(untouched, 2 * 60 * 60_000) is not None
