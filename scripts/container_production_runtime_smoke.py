"""Exercise the standalone production container against deterministic fake Bybit APIs."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import threading
import time
from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from market_data_service.adapters.sqlite import initialize_database

ROOT = Path(__file__).resolve().parents[1]
SERVICE = "market-data-service"
RUNTIME_UID = 10001
RUNTIME_GID = 10001
SMOKE_HTTP_PORT = 18080
DAY_MS = 86_400_000
INTERVAL_MS = {
    "5": 300_000,
    "60": 3_600_000,
    "240": 14_400_000,
    "D": DAY_MS,
}


def _run(
    args: Sequence[str],
    *,
    environment: dict[str, str] | None = None,
) -> str:
    completed = subprocess.run(
        args,
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        command = " ".join(args)
        raise RuntimeError(
            f"command failed ({completed.returncode}): {command}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed.stdout.strip()


class FakeBybitRest(AbstractContextManager["FakeBybitRest"]):
    def __init__(self) -> None:
        self.launch_time_ms = (int(time.time() * 1000) // DAY_MS - 1) * DAY_MS
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                if parsed.path.endswith("/v5/market/instruments-info"):
                    symbol = query["symbol"][0]
                    payload = parent._instrument_payload(symbol)
                elif parsed.path.endswith("/v5/market/kline"):
                    payload = parent._kline_payload(query)
                else:
                    self.send_error(404)
                    return
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def __enter__(self) -> FakeBybitRest:
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()

    def _instrument_payload(self, symbol: str) -> dict[str, object]:
        return {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": symbol,
                        "contractType": "LinearPerpetual",
                        "status": "Trading",
                        "settleCoin": "USDT",
                        "launchTime": str(self.launch_time_ms),
                    }
                ]
            },
        }

    def _kline_payload(self, query: dict[str, list[str]]) -> dict[str, object]:
        interval = query["interval"][0]
        step_ms = INTERVAL_MS[interval]
        start_ms = max(int(query["start"][0]), self.launch_time_ms)
        end_ms = int(query["end"][0])
        limit = int(query["limit"][0])
        first_ms = ((start_ms + step_ms - 1) // step_ms) * step_ms
        rows = [
            [str(open_ms), "100", "101", "99", "100.5", "10", "1000"]
            for open_ms in range(first_ms, end_ms + 1, step_ms)
        ]
        rows.reverse()
        return {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": rows[:limit]},
        }


class FakeBybitWebSocket(AbstractContextManager["FakeBybitWebSocket"]):
    def __init__(self) -> None:
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._port: int | None = None
        self._error: BaseException | None = None

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("fake WebSocket server has not started")
        return self._port

    def __enter__(self) -> FakeBybitWebSocket:
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("fake WebSocket server did not start")
        if self._error is not None:
            raise RuntimeError("fake WebSocket server failed") from self._error
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._stop.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("fake WebSocket server did not stop")

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:
            self._error = exc
            self._ready.set()

    async def _serve(self) -> None:
        async with serve(self._handle, "0.0.0.0", 0) as server:
            sockets = server.sockets
            if not sockets:
                raise RuntimeError("fake WebSocket server has no listening socket")
            self._port = int(sockets[0].getsockname()[1])
            self._ready.set()
            while not self._stop.is_set():
                await asyncio.sleep(0.05)

    async def _handle(self, connection: ServerConnection) -> None:
        async for raw in connection:
            payload = json.loads(raw)
            operation = payload.get("op")
            if operation not in {"subscribe", "unsubscribe"}:
                continue
            topics = payload.get("args", [])
            try:
                await connection.send(
                    json.dumps(
                        {
                            "success": True,
                            "op": operation,
                            "req_id": payload.get("req_id"),
                            "data": {"successTopics": topics},
                        }
                    )
                )
            except ConnectionClosed:
                return
            if operation == "unsubscribe":
                return


def _compose_command(project: str, override: Path) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        str(ROOT / "docker-compose.yml"),
        "--file",
        str(override),
    ]


def _write_override(
    path: Path,
    container_name: str,
    image_name: str,
    rest_port: int,
    ws_port: int,
) -> None:
    path.write_text(
        "\n".join(
            (
                "services:",
                "  market-data-service:",
                f"    container_name: {container_name}",
                f"    image: {image_name}",
                "    environment:",
                f'      MDS_REST_BASE_URL: "http://host.docker.internal:{rest_port}"',
                f'      MDS_WEBSOCKET_URL: "ws://host.docker.internal:{ws_port}"',
                f"      MDS_HTTP_PORT: {SMOKE_HTTP_PORT}",
                "      MDS_HISTORICAL_RETRY_BASE_SECONDS: 0",
                "      MDS_HISTORICAL_RETRY_MAX_SECONDS: 0",
                "      MDS_RECONNECT_DELAY_SECONDS: 0",
                "    extra_hosts:",
                '      - "host.docker.internal:host-gateway"',
                "    healthcheck:",
                "      interval: 1s",
                "      timeout: 5s",
                "      start_period: 1s",
                "      retries: 120",
                "    ports: !override",
                f"      - target: {SMOKE_HTTP_PORT}",
                "        host_ip: 127.0.0.1",
                "",
            )
        ),
        encoding="utf-8",
    )


def _wait_for_health(container_id: str, timeout_seconds: float = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status = "unknown"
    while time.monotonic() < deadline:
        last_status = _run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_id]
        )
        if last_status == "healthy":
            return
        time.sleep(0.5)
    logs = _run(["docker", "logs", container_id])
    raise RuntimeError(f"container health stayed {last_status}\n{logs}")


def _wait_for_exit(container_id: str, timeout_seconds: float = 30) -> int:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = json.loads(_run(["docker", "inspect", "--format", "{{json .State}}", container_id]))
        if not state["Running"]:
            return int(state["ExitCode"])
        time.sleep(0.25)
    raise RuntimeError(f"container {container_id} did not exit")


@dataclass(frozen=True)
class DurableStreamEvidence:
    earliest_open_time_ms: int | None
    latest_committed_open_time_ms: int | None
    last_audit_at_ms: int | None
    candles: tuple[tuple[object, ...], ...]


@dataclass(frozen=True)
class SqliteEvidence:
    schema_version: str
    streams: dict[str, DurableStreamEvidence]


def _sqlite_evidence(compose: list[str], environment: dict[str, str]) -> SqliteEvidence:
    probe = """
import json
import sqlite3

with sqlite3.connect("/data/market.sqlite3", timeout=30) as connection:
    version = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    stream_rows = connection.execute(
        '''
        SELECT streams.id, instruments.ticker, streams.timeframe,
               stream_state.earliest_available_open_time_ms,
               stream_state.latest_committed_open_time_ms,
               stream_state.last_audit_at_ms
        FROM streams
        JOIN instruments ON instruments.id = streams.instrument_id
        JOIN stream_state ON stream_state.stream_id = streams.id
        ORDER BY instruments.ticker, streams.timeframe
        '''
    ).fetchall()
    streams = []
    for stream_id, ticker, timeframe, earliest, latest, last_audit in stream_rows:
        candles = connection.execute(
            '''
            SELECT open_time_ms, open_value, high_value, low_value, close_value,
                   volume_value, source, committed_at_ms
            FROM candles
            WHERE stream_id = ?
            ORDER BY open_time_ms
            ''',
            (stream_id,),
        ).fetchall()
        streams.append(
            {
                "key": f"{ticker}:{timeframe}",
                "earliest": earliest,
                "latest": latest,
                "last_audit": last_audit,
                "candles": candles,
            }
        )
print(json.dumps({"schema_version": version[0], "streams": streams}))
"""
    raw = _run(
        [*compose, "exec", "--no-TTY", SERVICE, "python", "-c", probe],
        environment=environment,
    )
    payload = json.loads(raw)
    streams = {
        item["key"]: DurableStreamEvidence(
            earliest_open_time_ms=item["earliest"],
            latest_committed_open_time_ms=item["latest"],
            last_audit_at_ms=item["last_audit"],
            candles=tuple(tuple(row) for row in item["candles"]),
        )
        for item in payload["streams"]
    }
    evidence = SqliteEvidence(str(payload["schema_version"]), streams)
    if not evidence.streams or any(not item.candles for item in evidence.streams.values()):
        raise RuntimeError("real candle evidence is missing before restart")
    if any(
        item.latest_committed_open_time_ms is None or item.last_audit_at_ms is None
        for item in evidence.streams.values()
    ):
        raise RuntimeError("durable stream progress evidence is missing before restart")
    return evidence


def _assert_evidence_preserved(
    before: SqliteEvidence,
    after: SqliteEvidence,
    *,
    context: str,
) -> None:
    if after.schema_version != before.schema_version:
        raise RuntimeError(f"schema version changed {context}")
    if after.streams.keys() != before.streams.keys():
        raise RuntimeError(f"configured stream set changed {context}")
    for stream, previous in before.streams.items():
        current = after.streams[stream]
        current_by_open = {int(row[0]): row for row in current.candles}
        for candle in previous.candles:
            if current_by_open.get(int(candle[0])) != candle:
                raise RuntimeError(f"durable candle changed or disappeared for {stream} {context}")
        if len(current.candles) < len(previous.candles):
            raise RuntimeError(f"candle count regressed for {stream} {context}")
        if current.earliest_open_time_ms != previous.earliest_open_time_ms:
            raise RuntimeError(f"historical lower bound changed for {stream} {context}")
        _assert_progress_not_lower(
            previous.latest_committed_open_time_ms,
            current.latest_committed_open_time_ms,
            stream=stream,
            field="latest committed candle",
            context=context,
        )
        _assert_progress_not_lower(
            previous.last_audit_at_ms,
            current.last_audit_at_ms,
            stream=stream,
            field="last audit",
            context=context,
        )


def _assert_progress_not_lower(
    before: int | None,
    after: int | None,
    *,
    stream: str,
    field: str,
    context: str,
) -> None:
    if before is not None and (after is None or after < before):
        raise RuntimeError(f"{field} regressed for {stream} {context}")


def _set_data_permissions(
    image_name: str,
    data_directory: Path,
    *,
    uid: int,
    gid: int,
    directory_mode: int,
    file_mode: int,
) -> None:
    script = """
import os
import sys
from pathlib import Path

root = Path("/data")
uid, gid = int(sys.argv[1]), int(sys.argv[2])
directory_mode, file_mode = int(sys.argv[3], 8), int(sys.argv[4], 8)
for path in (*root.rglob("*"), root):
    os.chown(path, uid, gid)
    os.chmod(path, directory_mode if path.is_dir() else file_mode)
"""
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0:0",
            "--entrypoint",
            "python",
            "--mount",
            f"type=bind,source={data_directory},target=/data",
            image_name,
            "-c",
            script,
            str(uid),
            str(gid),
            f"{directory_mode:o}",
            f"{file_mode:o}",
        ]
    )


def _assert_existing_sqlite_is_writable(
    compose: list[str],
    environment: dict[str, str],
) -> None:
    probe = """
import json
import os
import sqlite3
import stat
from pathlib import Path

database = Path("/data/market.sqlite3")
with sqlite3.connect(database, timeout=30) as connection:
    connection.execute("BEGIN IMMEDIATE")
    schema_version = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()[0]
    connection.execute(
        "UPDATE schema_meta SET value = 'permission-probe' WHERE key = 'schema_version'"
    )
    connection.execute(
        "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
        (schema_version,),
    )
    connection.commit()
    paths = [database, Path(f"{database}-wal"), Path(f"{database}-shm")]
    facts = []
    for path in paths:
        info = path.stat()
        facts.append(
            {
                "name": path.name,
                "uid": info.st_uid,
                "gid": info.st_gid,
                "mode": stat.S_IMODE(info.st_mode),
                "writable": os.access(path, os.W_OK),
            }
        )
directory = database.parent.stat()
print(
    json.dumps(
        {
            "directory_mode": stat.S_IMODE(directory.st_mode),
            "directory_uid": directory.st_uid,
            "directory_gid": directory.st_gid,
            "files": facts,
        }
    )
)
"""
    raw = _run(
        [*compose, "exec", "--no-TTY", SERVICE, "python", "-c", probe],
        environment=environment,
    )
    facts = json.loads(raw)
    if facts["directory_mode"] != 0o750:
        raise RuntimeError(f"existing data directory mode is not 0750: {facts}")
    if facts["directory_uid"] != RUNTIME_UID or facts["directory_gid"] != RUNTIME_GID:
        raise RuntimeError(f"existing data directory ownership is wrong: {facts}")
    if {item["name"] for item in facts["files"]} != {
        "market.sqlite3",
        "market.sqlite3-wal",
        "market.sqlite3-shm",
    }:
        raise RuntimeError(f"SQLite WAL/SHM evidence is incomplete: {facts}")
    for item in facts["files"]:
        if item["uid"] != RUNTIME_UID or item["gid"] != RUNTIME_GID:
            raise RuntimeError(f"SQLite file ownership is wrong: {item}")
        if item["mode"] & 0o007 or not item["writable"]:
            raise RuntimeError(f"SQLite file permissions are too broad or not writable: {item}")


def _assert_image_contract(image_id: str) -> None:
    inspection = json.loads(_run(["docker", "image", "inspect", image_id]))[0]["Config"]
    if inspection["User"] != "10001:10001":
        raise RuntimeError(f"unexpected image user: {inspection['User']}")
    if inspection["Cmd"] != ["market-data-service", "serve"]:
        raise RuntimeError(f"unexpected image command: {inspection['Cmd']}")
    health_test = inspection["Healthcheck"]["Test"]
    health_command = " ".join(health_test)
    if (
        "/health" not in health_command
        or "/readiness" in health_command
        or "MDS_HTTP_PORT" not in health_command
        or "8080" not in health_command
    ):
        raise RuntimeError(f"unexpected image healthcheck: {health_test}")

    probe = """
import importlib.util
import os
from pathlib import Path

for forbidden in (
    "/app/.git",
    "/app/.venv",
    "/app/data",
    "/app/src",
    "/app/pyproject.toml",
    "/app/README.md",
):
    if Path(forbidden).exists():
        raise SystemExit(f"runtime-unnecessary image path exists: {forbidden}")
if importlib.util.find_spec("market_data_service") is None:
    raise SystemExit("installed package is missing")
if not Path("/app/config/markets.toml").is_file():
    raise SystemExit("built-in market config is missing")
try:
    Path("/app/.write-probe").write_text("unexpected", encoding="utf-8")
except OSError:
    pass
else:
    raise SystemExit("/app is writable")
probe_path = Path("/data/.write-probe")
probe_path.write_text("ok", encoding="utf-8")
probe_path.unlink()
if os.getuid() != 10001 or os.getgid() != 10001:
    raise SystemExit("runtime image process is not uid/gid 10001")
"""
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--read-only",
            "--entrypoint",
            "python",
            image_id,
            "-c",
            probe,
        ]
    )


def _assert_running_container_contract(compose: list[str], environment: dict[str, str]) -> str:
    container_id = _run([*compose, "ps", "--quiet", SERVICE], environment=environment)
    if not container_id:
        raise RuntimeError("Compose service container is missing")
    _wait_for_health(container_id)
    stop_timeout = _run(
        ["docker", "inspect", "--format", "{{.Config.StopTimeout}}", container_id]
    )
    if stop_timeout != "20":
        raise RuntimeError(f"container stop timeout is not production 20s: {stop_timeout}")
    pid1_probe = """
import json
from pathlib import Path

status = Path("/proc/1/status").read_text(encoding="utf-8")
uid = next(line for line in status.splitlines() if line.startswith("Uid:"))
gid = next(line for line in status.splitlines() if line.startswith("Gid:"))
cmdline = Path("/proc/1/cmdline").read_bytes().replace(b"\\0", b" ").decode()
print(json.dumps({"uid": uid.split()[1], "gid": gid.split()[1], "cmdline": cmdline}))
"""
    raw = _run(
        [*compose, "exec", "--no-TTY", SERVICE, "python", "-c", pid1_probe],
        environment=environment,
    )
    facts = json.loads(raw)
    if facts["uid"] != str(RUNTIME_UID) or facts["gid"] != str(RUNTIME_UID):
        raise RuntimeError(f"unexpected PID1 identity: {facts}")
    if "market-data-service" not in facts["cmdline"] or "serve" not in facts["cmdline"]:
        raise RuntimeError(f"unexpected PID1 command: {facts['cmdline']}")
    if " sh " in f" {facts['cmdline']} " or " bash " in f" {facts['cmdline']} ":
        raise RuntimeError(f"shell owns PID1: {facts['cmdline']}")
    return container_id


def _run_smoke(rest: FakeBybitRest, websocket: FakeBybitWebSocket) -> None:
    _run(["docker", "version", "--format", "{{.Server.Version}}"])
    temp_parent = ROOT / "tmp"
    temp_parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="container-smoke-", dir=temp_parent) as temp_name:
        temporary = Path(temp_name)
        data_root = temporary / "bbb-data"
        data_directory = data_root / "market-data"
        data_directory.mkdir(parents=True)
        database = data_directory / "market.sqlite3"
        initialize_database(database)
        override = temporary / "compose-smoke.yml"
        project = f"mds-container-smoke-{os.getpid()}"
        container_name = f"{project}-service"
        image_name = f"{project}:test"
        _write_override(override, container_name, image_name, rest.port, websocket.port)
        compose = _compose_command(project, override)
        environment = {**os.environ, "BBB_DATA_ROOT": str(data_root)}
        image_built = False
        try:
            _run([*compose, "build", SERVICE], environment=environment)
            image_built = True
            _assert_image_contract(image_name)
            _set_data_permissions(
                image_name,
                data_directory,
                uid=RUNTIME_UID,
                gid=RUNTIME_GID,
                directory_mode=0o750,
                file_mode=0o640,
            )

            _run([*compose, "up", "--detach", SERVICE], environment=environment)
            container_id = _assert_running_container_contract(compose, environment)
            _assert_existing_sqlite_is_writable(compose, environment)
            initial = _sqlite_evidence(compose, environment)

            _run([*compose, "restart", SERVICE], environment=environment)
            container_id = _assert_running_container_contract(compose, environment)
            after_restart = _sqlite_evidence(compose, environment)
            _assert_evidence_preserved(initial, after_restart, context="after restart")

            _run(
                [*compose, "up", "--detach", "--force-recreate", "--no-deps", SERVICE],
                environment=environment,
            )
            container_id = _assert_running_container_contract(compose, environment)
            after_recreate = _sqlite_evidence(compose, environment)
            _assert_evidence_preserved(
                after_restart,
                after_recreate,
                context="after recreation",
            )

            _run(["docker", "update", "--restart=no", container_id])
            _run(["docker", "kill", "--signal=SIGINT", container_id])
            if _wait_for_exit(container_id) != 0:
                raise RuntimeError("SIGINT shutdown did not exit cleanly")

            _run([*compose, "up", "--detach", SERVICE], environment=environment)
            container_id = _assert_running_container_contract(compose, environment)
            _run([*compose, "stop", SERVICE], environment=environment)
            if _wait_for_exit(container_id) != 0:
                raise RuntimeError("SIGTERM shutdown did not exit cleanly")
        finally:
            _run([*compose, "down", "--remove-orphans"], environment=environment)
            if image_built:
                _set_data_permissions(
                    image_name,
                    data_directory,
                    uid=os.getuid(),
                    gid=os.getgid(),
                    directory_mode=0o700,
                    file_mode=0o600,
                )
                subprocess.run(
                    ["docker", "image", "rm", image_name],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )


def main() -> int:
    with FakeBybitRest() as rest, FakeBybitWebSocket() as websocket:
        _run_smoke(rest, websocket)
    print("container production runtime smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
