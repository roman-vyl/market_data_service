from __future__ import annotations

from dataclasses import dataclass

from market_data_service.application.full_bootstrap import FullHistoryBootstrapResult
from market_data_service.application.multi_stream_backfill import (
    BackfillAllConfiguredStreams,
    MultiStreamBackfillRequest,
)
from market_data_service.domain import (
    HistoryPolicy,
    InstrumentCoverage,
    InstrumentKey,
    StreamKey,
)


def _coverage(ticker: str, timeframes: tuple[str, ...] = ("1m",)) -> InstrumentCoverage:
    return InstrumentCoverage(
        instrument=InstrumentKey(ticker),
        exchange_symbol=ticker.removesuffix(".P"),
        enabled=True,
        canonical_timeframes=timeframes,
        history_policy=HistoryPolicy.FULL_AVAILABLE,
    )


def _result(
    stream: StreamKey,
    *,
    reached: bool = False,
    error: str | None = None,
    disposition: str | None = None,
) -> FullHistoryBootstrapResult:
    return FullHistoryBootstrapResult(
        stream=stream,
        status="incomplete" if not reached else "backfilled",
        max_windows=2,
        target_open_time_ms=60_000,
        lower_bound=None,
        backfill=None,
        error_code=error,
        error_detail=error,
        failure_disposition=disposition,  # type: ignore[arg-type]
    )


@dataclass
class FakeBootstrap:
    result: FullHistoryBootstrapResult
    order: list[str]

    def execute(self, request):  # type: ignore[no-untyped-def]
        self.order.append(request.stream.canonical_id)
        return self.result


def test_all_streams_run_for_every_ticker_timeframe_in_configuration_order() -> None:
    btc = _coverage("BTCUSDT.P", ("1m", "5m", "1h"))
    eth = _coverage("ETHUSDT.P", ("1m", "5m"))
    order: list[str] = []

    run = BackfillAllConfiguredStreams(
        lambda coverage: coverage,
        lambda coverage, stream: FakeBootstrap(_result(stream), order),  # type: ignore[arg-type]
    ).execute((btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=2))

    assert order == [
        "BTCUSDT.P:1m",
        "BTCUSDT.P:5m",
        "BTCUSDT.P:1h",
        "ETHUSDT.P:1m",
        "ETHUSDT.P:5m",
    ]
    assert [item.stream.canonical_id for item in run.outcomes] == order
    assert run.status == "incomplete"


def test_recoverable_stream_failure_continues_to_later_streams() -> None:
    btc = _coverage("BTCUSDT.P", ("1m", "5m"))
    eth = _coverage("ETHUSDT.P", ("1m",))
    order: list[str] = []

    def factory(coverage: InstrumentCoverage, stream: StreamKey) -> FakeBootstrap:
        if stream.canonical_id == "BTCUSDT.P:1m":
            result = _result(stream, error="timeout", disposition="recoverable")
        else:
            result = _result(stream)
        return FakeBootstrap(result, order)

    run = BackfillAllConfiguredStreams(lambda coverage: coverage, factory).execute(
        (btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=1)
    )

    assert order == ["BTCUSDT.P:1m", "BTCUSDT.P:5m", "ETHUSDT.P:1m"]
    assert run.attempted_streams == 3
    assert run.has_errors is True
    assert run.status == "incomplete"


def test_fatal_stream_failure_stops_before_later_streams() -> None:
    btc = _coverage("BTCUSDT.P", ("1m", "5m"))
    eth = _coverage("ETHUSDT.P", ("1m",))
    order: list[str] = []

    def factory(coverage: InstrumentCoverage, stream: StreamKey) -> FakeBootstrap:
        disposition = "fatal" if stream.canonical_id == "BTCUSDT.P:5m" else None
        error = "schema" if disposition else None
        return FakeBootstrap(_result(stream, error=error, disposition=disposition), order)

    run = BackfillAllConfiguredStreams(lambda coverage: coverage, factory).execute(
        (btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=1)
    )

    assert order == ["BTCUSDT.P:1m", "BTCUSDT.P:5m"]
    assert run.status == "failed"


def test_recoverable_metadata_failure_reports_all_instrument_streams_and_continues() -> None:
    from market_data_service.adapters.bybit.errors import BybitHttpError

    btc = _coverage("BTCUSDT.P", ("1m", "5m"))
    eth = _coverage("ETHUSDT.P", ("1m",))
    order: list[str] = []

    def verify(coverage: InstrumentCoverage) -> object:
        if coverage.instrument == btc.instrument:
            raise BybitHttpError("timeout")
        return coverage

    run = BackfillAllConfiguredStreams(
        verify,
        lambda coverage, stream: FakeBootstrap(_result(stream), order),  # type: ignore[arg-type]
    ).execute((btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=1))

    assert [item.stream.canonical_id for item in run.outcomes] == [
        "BTCUSDT.P:1m",
        "BTCUSDT.P:5m",
        "ETHUSDT.P:1m",
    ]
    assert order == ["ETHUSDT.P:1m"]
    assert run.outcomes[0].failure_disposition == "recoverable"
    assert run.outcomes[1].failure_disposition == "recoverable"


def test_fatal_metadata_mismatch_stops_later_instruments() -> None:
    from market_data_service.application.market_metadata import InstrumentMetadataMismatch

    btc = _coverage("BTCUSDT.P", ("1m", "5m"))
    eth = _coverage("ETHUSDT.P", ("1m",))
    order: list[str] = []

    def verify(coverage: InstrumentCoverage) -> object:
        if coverage.instrument == btc.instrument:
            raise InstrumentMetadataMismatch("symbol mismatch")
        return coverage

    run = BackfillAllConfiguredStreams(
        verify,
        lambda coverage, stream: FakeBootstrap(_result(stream), order),  # type: ignore[arg-type]
    ).execute((btc, eth), MultiStreamBackfillRequest(max_windows_per_stream=1))

    assert order == []
    assert run.status == "failed"
    assert [item.stream.canonical_id for item in run.outcomes] == [
        "BTCUSDT.P:1m",
        "BTCUSDT.P:5m",
    ]
