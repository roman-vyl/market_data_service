"""Integration smoke: a real MDS process with `open_interest = true` loads OI by itself.

Runs `python -m market_data_service serve` against a fake Bybit REST
API and a fake silent WebSocket. No manual command is used: the flag alone must
start the open interest track, while candles, realtime and readiness keep working.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from tests.fake_bybit_api import FakeBybitApi, FakeBybitState
from websockets.asyncio.server import ServerConnection, serve

STEP = 300_000
BARS = 700
OI_FROM_BAR = 250

CONFIG = """
schema_version = 1
[source]
venue = "bybit"
category = "linear"
[[instruments]]
ticker = "BTCUSDT.P"
exchange_symbol = "BTCUSDT"
enabled = true
canonical_timeframes = ["5m"]
history_policy = "full_available"
open_interest = true
[[instruments]]
ticker = "ETHUSDT.P"
exchange_symbol = "ETHUSDT"
enabled = true
canonical_timeframes = ["5m"]
history_policy = "full_available"
"""


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def _silent_websocket() -> Iterator[str]:
    port = _free_port()
    ready = threading.Event()
    stop: dict[str, asyncio.Future[None]] = {}

    async def handler(connection: ServerConnection) -> None:
        async for raw in connection:
            message = json.loads(raw)
            if message.get("op") == "subscribe":
                await connection.send(
                    json.dumps({"op": "subscribe", "success": True, "req_id": message["req_id"]})
                )
            elif message.get("op") == "ping":
                await connection.send(json.dumps({"op": "pong", "success": True}))

    async def main() -> None:
        stop["future"] = asyncio.get_running_loop().create_future()
        async with serve(handler, "127.0.0.1", port):
            ready.set()
            await stop["future"]

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_until_complete, args=(main(),), daemon=True)
    thread.start()
    ready.wait(5)
    try:
        yield f"ws://127.0.0.1:{port}"
    finally:
        loop.call_soon_threadsafe(stop["future"].set_result, None)
        thread.join(5)


def _seed(state: FakeBybitState) -> int:
    now = int(time.time() * 1000)
    last_closed = (now // STEP) * STEP - STEP
    start = last_closed - (BARS - 1) * STEP
    for symbol, base in (("BTCUSDT", 60_000), ("ETHUSDT", 3_000)):
        # Seed one hour ahead so bars closing during the test exist too.
        state.seed_stream(symbol, "5", start_ms=start, count=BARS + 12, step_ms=STEP, base=base)
    state.open_interest["BTCUSDT"] = {
        start + index * STEP: f"{80_000 + index}.25" for index in range(OI_FROM_BAR, BARS + 12)
    }
    return start


def _start(env: dict[str, str], log_path: Path) -> subprocess.Popen[bytes]:
    log = log_path.open("ab")
    return subprocess.Popen(
        [sys.executable, "-m", "market_data_service", "serve"],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def _wait_for(predicate, timeout: float, what: str) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def _readiness(port: int) -> dict[str, object]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readiness", timeout=2) as response:
            return json.loads(response.read())  # type: ignore[no-any-return]
    except Exception as exc:  # 503 while not ready
        body = getattr(exc, "read", lambda: b"{}")()
        return json.loads(body or b"{}")  # type: ignore[no-any-return]


def _query(path: Path, sql: str) -> list[tuple[object, ...]]:
    connection = sqlite3.connect(path)
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


def _stop(process: subprocess.Popen[bytes]) -> None:
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(5)


def test_flag_alone_loads_open_interest_inside_the_running_service(tmp_path: Path) -> None:
    state = FakeBybitState()
    start = _seed(state)
    database = tmp_path / "market.sqlite3"
    config = tmp_path / "markets.toml"
    config.write_text(CONFIG, encoding="utf-8")
    log_path = tmp_path / "mds.log"
    port = _free_port()
    with FakeBybitApi(state) as api, _silent_websocket() as ws_url:
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [str(Path(__file__).parents[1] / "src"), os.environ.get("PYTHONPATH", "")]
            ),
            "MDS_DATABASE_PATH": str(database),
            "MDS_MARKETS_CONFIG_PATH": str(config),
            "MDS_HTTP_PORT": str(port),
            "MDS_REST_BASE_URL": api.base_url,
            "MDS_WEBSOCKET_URL": ws_url,
            "MDS_OI_TRACK_REFRESH_SECONDS": "1",
        }
        process = _start(env, log_path)
        try:
            _wait_for(
                lambda: "oi track complete stream=BTCUSDT.P" in log_path.read_text(),
                60,
                "the first complete open interest cycle",
            )
            _wait_for(lambda: _readiness(port).get("ready") is True, 30, "service readiness")
        finally:
            _stop(process)
        log_first = log_path.read_text()

        btc_oi = _query(
            database,
            "SELECT COUNT(*), MIN(open_time_ms) FROM open_interest o "
            "JOIN streams s ON s.id = o.stream_id JOIN instruments i ON i.id = s.instrument_id "
            "WHERE i.ticker = 'BTCUSDT.P'",
        )[0]
        eth_oi = _query(
            database,
            "SELECT COUNT(*) FROM open_interest o JOIN streams s ON s.id = o.stream_id "
            "JOIN instruments i ON i.id = s.instrument_id WHERE i.ticker = 'ETHUSDT.P'",
        )[0]
        progress = _query(
            database,
            "SELECT earliest_available_open_time_ms, last_audit_gap_count, "
            "latest_committed_open_time_ms FROM track_progress",
        )
        candles = _query(database, "SELECT COUNT(*) FROM candles")[0][0]
        states = _query(database, "SELECT state FROM stream_state ORDER BY stream_id")
        latest_oi = progress[0][2]
        expected_oi = (int(latest_oi) - (start + OI_FROM_BAR * STEP)) // STEP + 1  # type: ignore[arg-type]
        oi_symbols = {
            query["symbol"][0] for path, query in state.calls if path.endswith("open-interest")
        }

        assert btc_oi == (expected_oi, start + OI_FROM_BAR * STEP)
        assert expected_oi >= BARS - OI_FROM_BAR
        assert eth_oi == (0,)
        assert progress[0][:2] == (start + OI_FROM_BAR * STEP, 0)
        assert candles >= 2 * BARS
        assert states == [("ready",), ("ready",)]
        assert oi_symbols == {"BTCUSDT"}
        assert "schema" not in log_first.lower() or "unsupported" not in log_first.lower()
        assert "Traceback" not in log_first

        # Restart: the track resumes from stored data and only fetches the tail.
        state.calls.clear()
        process = _start(env, log_path)
        try:
            _wait_for(
                lambda: (
                    log_path.read_text().count("oi track complete stream=BTCUSDT.P")
                    > log_first.count("oi track complete stream=BTCUSDT.P")
                ),
                60,
                "the open interest cycle after restart",
            )
        finally:
            _stop(process)
        restart_calls = [query for path, query in state.calls if path.endswith("open-interest")]
        # Lower bound is cached, the stored history is not fetched again: at most a tail window.
        assert len(restart_calls) <= 2
        assert "Traceback" not in log_path.read_text()
