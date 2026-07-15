from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from market_data_service.adapters.http import RuntimeHttpServer
from market_data_service.adapters.http.consumer_read import ConsumerReadHttpHandler
from market_data_service.adapters.http.historical_read import HistoricalReadHttpHandler
from market_data_service.adapters.http.history_planning import HistoryPlanningHttpHandler
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.consumer_candle_reader import SqliteConsumerCandleReader
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.consumer_read import (
    GetCandleRange,
    GetHistoricalCandleRange,
)
from market_data_service.application.consumer_read.provenance import canonical_market_data_hash
from market_data_service.application.history_planning import AuditStreamRange, GetStreamBounds
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.status import RuntimeStatusStore

STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
CONFIG = ValidatedMarketConfig(
    1,
    MarketSourceConfig("bybit", "linear"),
    (
        InstrumentCoverage(
            STREAM.instrument,
            "BTCUSDT",
            True,
            ("1m",),
            HistoryPolicy.FULL_AVAILABLE,
        ),
    ),
)


def _request(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, object] | None = None,
) -> tuple[int, dict[str, object]]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        url,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urlopen(request, timeout=2)
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())
    return response.status, json.loads(response.read())


def _seed(database: Path, open_times_ms: tuple[int, ...]) -> None:
    initialize_database(database)
    register_stream(database, STREAM, exchange_symbol="BTCUSDT", now_ms=1)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    for open_time_ms in open_times_ms:
        ingest.execute(
            ObservedCandle(
                stream=STREAM,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open="1.2300",
                high="2",
                low="1",
                close="1.500",
                volume="10.500",
                confirmed=True,
                observed_at_ms=open_time_ms + 60_000,
                source=ObservationSource.BYBIT_REST,
            ),
            committed_at_ms=open_time_ms + 60_001,
        )
    with SqliteUnitOfWork(database) as unit_of_work:
        state = unit_of_work.get_stream_state(STREAM)
        unit_of_work.save_stream_state(
            replace(
                state,
                state=StreamLifecycleState.DEGRADED,
                earliest_available_open_time_ms=min(open_times_ms),
                latest_committed_open_time_ms=max(open_times_ms),
            )
        )
        unit_of_work.commit()


def _server(database: Path) -> RuntimeHttpServer:
    def unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    history = HistoryPlanningHttpHandler(
        CONFIG,
        GetStreamBounds(unit_of_work),
        AuditStreamRange(
            AuditStreamContinuity(unit_of_work),
            unit_of_work,
            SqliteConsumerCandleReader(database),
        ),
    )
    return RuntimeHttpServer(
        "127.0.0.1",
        0,
        RuntimeStatusStore((STREAM,)),
        ConsumerReadHttpHandler(GetCandleRange(CONFIG, SqliteConsumerCandleReader(database))),
        history,
        HistoricalReadHttpHandler(
            GetHistoricalCandleRange(
                CONFIG,
                SqliteConsumerCandleReader(database),
            )
        ),
    )


def test_bounds_and_audit_are_read_only_and_ignore_global_readiness(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0, 60_000, 120_000))
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, bounds = _request(f"http://{host}:{port}/v1/streams/BTCUSDT.P/1m/bounds")
        assert code == 200
        assert bounds == {
            "contract_version": "market_stream_bounds.v1",
            "ticker": "BTCUSDT.P",
            "timeframe": "1m",
            "state": "degraded",
            "earliest_committed_open_time_ms": 0,
            "latest_committed_open_time_ms": 120_000,
        }

        code, audit = _request(
            f"http://{host}:{port}/v1/streams/BTCUSDT.P/1m/continuity-audits",
            method="POST",
            body={"start_time_ms": 0, "end_time_ms": 180_000},
        )
        assert code == 200
        assert audit["is_continuous"] is True
        assert audit["candle_count"] == 3
        assert audit["state"] == "degraded"
        assert audit["gaps"] == []

        with SqliteUnitOfWork(database) as unit_of_work:
            assert unit_of_work.get_stream_state(STREAM).state is StreamLifecycleState.DEGRADED
    finally:
        server.close()


def test_audit_reports_exact_gap_without_repair(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0, 120_000))
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, audit = _request(
            f"http://{host}:{port}/v1/streams/BTCUSDT.P/1m/continuity-audits",
            method="POST",
            body={"start_time_ms": 0, "end_time_ms": 180_000},
        )
        assert code == 200
        assert audit["is_continuous"] is False
        assert audit["gaps"] == [{"from_ms": 60_000, "to_ms": 120_000}]

        with SqliteUnitOfWork(database) as unit_of_work:
            candles = unit_of_work.list_candles(
                STREAM,
                start_time_ms=0,
                end_time_ms=180_000,
            )
            assert [candle.open_time_ms for candle in candles] == [0, 120_000]
    finally:
        server.close()


def test_candle_response_contains_mds_owned_canonical_hash(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0, 60_000))
    with SqliteUnitOfWork(database) as unit_of_work:
        state = unit_of_work.get_stream_state(STREAM)
        unit_of_work.save_stream_state(replace(state, state=StreamLifecycleState.READY))
        unit_of_work.commit()
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, payload = _request(
            f"http://{host}:{port}/v1/candles?ticker=BTCUSDT.P&timeframe=1m&from_ms=0&to_ms=120000"
        )
        assert code == 200
        with SqliteUnitOfWork(database) as unit_of_work:
            candles = unit_of_work.list_candles(
                STREAM,
                start_time_ms=0,
                end_time_ms=120_000,
            )
        assert payload["market_data_hash"] == canonical_market_data_hash(
            stream=STREAM,
            from_ms=0,
            to_ms=120_000,
            candles=candles,
        )
        assert len(str(payload["market_data_hash"])) == 64
    finally:
        server.close()


def test_history_contract_rejects_unaligned_or_malformed_requests(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0,))
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, payload = _request(
            f"http://{host}:{port}/v1/streams/BTCUSDT.P/1m/continuity-audits",
            method="POST",
            body={"start_time_ms": 1, "end_time_ms": 60_000},
        )
        assert code == 422
        assert payload["error"] == "invalid_request"

        code, payload = _request(f"http://{host}:{port}/v1/streams/ETHUSDT.P/1m/bounds")
        assert code == 404
        assert payload["error"] == "stream_not_found"
    finally:
        server.close()


def test_historical_read_allows_degraded_stream_with_matching_audit_hash(
    tmp_path: Path,
) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0, 60_000, 120_000))
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, audit = _request(
            f"http://{host}:{port}/v1/streams/BTCUSDT.P/1m/continuity-audits",
            method="POST",
            body={"from_ms": 0, "to_ms": 180_000},
        )
        assert code == 200
        assert audit["is_continuous"] is True
        expected_hash = audit["market_data_hash"]
        assert isinstance(expected_hash, str)

        code, payload = _request(
            f"http://{host}:{port}/v1/historical-candles",
            method="POST",
            body={
                "ticker": "BTCUSDT.P",
                "timeframe": "1m",
                "from_ms": 0,
                "to_ms": 180_000,
                "expected_market_data_hash": expected_hash,
            },
        )
        assert code == 200
        assert payload["market_data_hash"] == expected_hash
        assert len(payload["candles"]) == 3
    finally:
        server.close()


def test_historical_read_rejects_stale_hash(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite3"
    _seed(database, (0, 60_000))
    server = _server(database)
    server.start()
    host, port = server.address
    try:
        code, payload = _request(
            f"http://{host}:{port}/v1/historical-candles",
            method="POST",
            body={
                "ticker": "BTCUSDT.P",
                "timeframe": "1m",
                "from_ms": 0,
                "to_ms": 120_000,
                "expected_market_data_hash": "0" * 64,
            },
        )
        assert code == 409
        assert payload["error"] == "coverage_stale"
    finally:
        server.close()
