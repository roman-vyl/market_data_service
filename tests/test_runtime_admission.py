from __future__ import annotations

from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.runtime.admission import (
    AdmissionGatedCandleHandler,
    RealtimeAdmissionGate,
)


class FakeHandler:
    def __init__(self) -> None:
        self.calls = 0

    def handle(self, event: CandleObserved) -> RealtimeIngestionOutcome:
        self.calls += 1
        return RealtimeIngestionOutcome(
            event.stream,
            event.candle.open_time_ms,
            RealtimeIngestionClassification.COMMITTED,
        )


def test_incomplete_stream_is_not_sent_to_canonical_realtime_ingestion() -> None:
    stream = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    candle = ObservedCandle(
        stream=stream,
        open_time_ms=0,
        close_time_ms=299_999,
        open="1",
        high="2",
        low="1",
        close="2",
        volume="3",
        confirmed=True,
        observed_at_ms=300_000,
        source=ObservationSource.BYBIT_WEBSOCKET,
    )
    event = CandleObserved(stream, candle)
    gate = RealtimeAdmissionGate()
    inner = FakeHandler()
    handler = AdmissionGatedCandleHandler(gate, inner)

    assert handler.handle(event) is None
    assert inner.calls == 0

    gate.admit(stream)
    assert handler.handle(event) is not None
    assert inner.calls == 1
