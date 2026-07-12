from __future__ import annotations

from market_data_service.application.ingest import IngestionResult
from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import RealtimeIngestionClassification
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.classification import IngestionClassification


class FakeIngestion:
    def __init__(self, result: IngestionResult | Exception) -> None:
        self.result = result
        self.calls = 0

    def execute(self, candle: ObservedCandle, *, committed_at_ms: int) -> IngestionResult:
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _event(*, confirmed: bool = True) -> CandleObserved:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
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
            confirmed=confirmed,
            observed_at_ms=60_000,
            source=ObservationSource.BYBIT_WEBSOCKET,
        ),
    )


def test_unconfirmed_update_is_ignored() -> None:
    ingestion = FakeIngestion(IngestionResult(IngestionClassification.COMMITTED))
    handler = RealtimeCandleHandler(ingestion, lambda: 1)  # type: ignore[arg-type]

    assert handler.handle(_event(confirmed=False)) is None
    assert ingestion.calls == 0


def test_confirmed_update_reports_canonical_outcome() -> None:
    ingestion = FakeIngestion(IngestionResult(IngestionClassification.DUPLICATE))
    handler = RealtimeCandleHandler(ingestion, lambda: 1)  # type: ignore[arg-type]

    outcome = handler.handle(_event())

    assert outcome is not None
    assert outcome.classification is RealtimeIngestionClassification.DUPLICATE
    assert ingestion.calls == 1


def test_storage_failure_is_reported() -> None:
    handler = RealtimeCandleHandler(FakeIngestion(RuntimeError("disk unavailable")), lambda: 1)  # type: ignore[arg-type]

    outcome = handler.handle(_event())

    assert outcome is not None
    assert outcome.classification is RealtimeIngestionClassification.FAILED
    assert outcome.error_code == "RuntimeError"
