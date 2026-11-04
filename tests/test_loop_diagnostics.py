from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import SqliteUnitOfWork, initialize_database
from market_data_service.ports.operation_timing import (
    OperationSpan,
    measure_operation,
    measure_phase,
    observe_operations,
)
from market_data_service.runtime.loop_diagnostics import LoopDiagnostics
from market_data_service.runtime.settings import RuntimeSettings

_MS = 1_000_000
_HOUR = 3_600_000_000_000


def test_measures_sqlite_phases_at_call_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "timing.sqlite3"
    initialize_database(path)
    now_ns = 0
    observed: list[OperationSpan] = []

    def advance(ms: int) -> None:
        nonlocal now_ns
        now_ns += ms * _MS

    class TimedConnection(sqlite3.Connection):
        def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
            if sql == "BEGIN IMMEDIATE":
                advance(600)
            return super().execute(sql, parameters)

        def commit(self) -> None:
            advance(300)
            super().commit()

    def connect(path: Path) -> sqlite3.Connection:
        advance(70)
        return sqlite3.connect(path, factory=TimedConnection)

    monkeypatch.setattr(
        "market_data_service.ports.operation_timing.perf_counter_ns", lambda: now_ns
    )
    monkeypatch.setattr("market_data_service.adapters.sqlite.transaction.open_connection", connect)

    with observe_operations(observed.append):
        with measure_operation("ws.ingest"), SqliteUnitOfWork(path) as unit:
            with measure_phase("transaction_body"):
                advance(20)
            unit.commit()
        with SqliteUnitOfWork(path) as unit:
            unit.commit()

    assert {span.phase: span.duration_ns // _MS for span in observed} == {
        "sqlite.open": 70,
        "sqlite.begin_immediate": 600,
        "transaction_body": 20,
        "sqlite.commit": 300,
        "total": 990,
    }
    assert len(observed) == 5


def test_lag_accounts_for_nested_work_and_short_lifecycle_reads() -> None:
    diagnostics = LoopDiagnostics(logging.getLogger(__name__), now_ns=0)
    for i in range(40):
        diagnostics.spans.record(
            OperationSpan("lifecycle.sync", "snapshot", None, i * 10 * _MS, (i + 1) * 10 * _MS)
        )
    diagnostics.spans.record(
        OperationSpan("lifecycle.sync", "transition", None, 400 * _MS, 600 * _MS)
    )
    diagnostics.spans.record(OperationSpan("lifecycle.sync", "total", None, 0, 600 * _MS))
    diagnostics.tick(100 * _MS, 1000 * _MS, warn=True)
    name, fields = diagnostics._output.get_nowait()
    assert name == "event loop lag"
    assert (fields["lag_ms"], fields["covered_ms"], fields["unexplained_ms"]) == (900, 500, 400)
    operations = fields["operations"]
    assert isinstance(operations, list)
    assert operations[0]["reads"] == {"count": 40, "total_ms": 400, "max_ms": 10, "slow": 0}
    assert operations[1]["phase"] == "transition"

    diagnostics.tick(_HOUR, _HOUR + 1200 * _MS, warn=True)
    _, uncovered = diagnostics._output.get_nowait()
    assert uncovered["operations"] == []
    assert uncovered["unexplained_ms"] == 1200
    name, aggregate = diagnostics._output.get_nowait()
    assert name == "event loop diagnostics"
    assert aggregate["coverage"] == {"full": 0, "partial": 1, "zero": 1, "incomplete": 0}
    assert {
        "operation": "lifecycle.sync",
        "phase": "snapshot",
        "count": 40,
        "total_ms": 400,
        "max_ms": 10,
        "slow": 0,
    } in aggregate["phases"]


def test_consecutive_short_operations_cover_lag_without_retaining_short_phases() -> None:
    diagnostics = LoopDiagnostics(logging.getLogger(__name__), capacity=40, now_ns=0)
    for i in range(40):
        start = i * 30 * _MS
        end = start + 30 * _MS
        operation = "ws.ingest" if i % 2 else "oi.handoff"
        diagnostics.spans.record(
            OperationSpan(operation, "sqlite.begin_immediate", None, start, end)
        )
        diagnostics.spans.record(OperationSpan(operation, "total", None, start, end))
    diagnostics.tick(0, 1200 * _MS, warn=True)
    _, fields = diagnostics._output.get_nowait()
    assert fields["coverage"] == "full"
    assert fields["covered_ms"] == 1200
    assert fields["unexplained_ms"] == 0
    assert fields["lost_measurements"] == 0


def test_overflow_marks_measurements_and_output_loss() -> None:
    diagnostics = LoopDiagnostics(logging.getLogger(__name__), capacity=1, now_ns=0)
    diagnostics.spans.record(OperationSpan("ws.ingest", "sqlite.open", None, 0, 100 * _MS))
    diagnostics.spans.record(OperationSpan("ws.ingest", "total", None, 0, 200 * _MS))
    diagnostics.tick(0, 200 * _MS, warn=True)
    diagnostics.tick(200 * _MS, 300 * _MS, warn=True)
    _, first = diagnostics._output.get_nowait()
    assert first["covered_ms"] == 200
    assert first["coverage"] == "incomplete"
    assert first["incomplete"] is True
    assert first["lost_measurements"] == 1
    diagnostics.tick(300 * _MS, 400 * _MS, warn=True)
    _, next_event = diagnostics._output.get_nowait()
    assert next_event["lost_output_events"] == 1
    assert next_event["incomplete"] is False
    diagnostics.tick(_HOUR, _HOUR, warn=False)
    _, aggregate = diagnostics._output.get_nowait()
    assert aggregate["coverage"] == {"full": 0, "partial": 0, "zero": 2, "incomplete": 1}


def test_disabled_diagnostics_do_not_read_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MDS_LOOP_DIAGNOSTICS_ENABLED", raising=False)
    assert not RuntimeSettings.from_environment().loop_diagnostics_enabled

    def unexpected_clock() -> int:
        raise AssertionError("disabled operation must not read diagnostic clock")

    monkeypatch.setattr(
        "market_data_service.ports.operation_timing.perf_counter_ns", unexpected_clock
    )
    with measure_operation("ws.ingest"), measure_phase("sqlite.commit"):
        pass
    monkeypatch.setenv("MDS_LOOP_DIAGNOSTICS_ENABLED", "true")
    assert RuntimeSettings.from_environment().loop_diagnostics_enabled


def test_lag_logging_runs_off_the_loop() -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop_thread = threading.get_ident()
        emissions: list[tuple[int, str]] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                emissions.append((threading.get_ident(), record.getMessage()))
                loop.call_soon_threadsafe(stop.set)

        logger = logging.Logger("diagnostic-test")
        logger.addHandler(Capture())
        diagnostics = LoopDiagnostics(logger, now_ns=0)
        diagnostics.tick(0, 1500 * _MS, warn=True)
        assert emissions == []
        await asyncio.wait_for(diagnostics.emit(stop), timeout=2)
        assert len(emissions) == 1
        thread_id, message = emissions[0]
        assert thread_id != loop_thread
        assert message.startswith("event loop lag 1.50s")
        assert '"unexplained_ms":1500.0' in message

    asyncio.run(scenario())
