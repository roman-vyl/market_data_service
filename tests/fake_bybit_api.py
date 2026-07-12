from __future__ import annotations

import json
import threading
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


@dataclass
class FakeBybitState:
    candles: dict[tuple[str, str], dict[int, list[str]]] = field(default_factory=dict)
    launch_times: dict[str, int] = field(default_factory=dict)
    transient_kline_failures: int = 0
    transient_failures_by_stream: dict[tuple[str, str], int] = field(default_factory=dict)
    calls: list[tuple[str, dict[str, list[str]]]] = field(default_factory=list)

    def seed_stream(
        self,
        symbol: str,
        interval: str,
        *,
        start_ms: int,
        count: int,
        step_ms: int,
        base: int,
    ) -> None:
        rows: dict[int, list[str]] = {}
        for index in range(count):
            open_time = start_ms + index * step_ms
            value = base + index
            rows[open_time] = [
                str(open_time),
                str(value),
                str(value + 2),
                str(value - 1),
                str(value + 1),
                str(10 + index),
                "0",
            ]
        self.candles[(symbol, interval)] = rows
        self.launch_times.setdefault(symbol, start_ms)

    def seed_symbol(self, symbol: str, *, start_ms: int, count: int, base: int) -> None:
        self.seed_stream(
            symbol,
            "1",
            start_ms=start_ms,
            count=count,
            step_ms=60_000,
            base=base,
        )

    def remove(self, symbol: str, *open_times: int, interval: str = "1") -> None:
        for open_time in open_times:
            self.candles[(symbol, interval)].pop(open_time, None)

    def mutate_close(
        self,
        symbol: str,
        open_time: int,
        close_value: str,
        *,
        interval: str = "1",
    ) -> None:
        self.candles[(symbol, interval)][open_time][4] = close_value


class FakeBybitApi(AbstractContextManager["FakeBybitApi"]):
    def __init__(self, state: FakeBybitState) -> None:
        self.state = state
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                parent.state.calls.append((parsed.path, query))
                if parsed.path.endswith("/v5/market/instruments-info"):
                    symbol = query["symbol"][0]
                    payload = {
                        "retCode": 0,
                        "retMsg": "OK",
                        "result": {
                            "list": [
                                {
                                    "symbol": symbol,
                                    "contractType": "LinearPerpetual",
                                    "status": "Trading",
                                    "settleCoin": "USDT",
                                    "launchTime": str(parent.state.launch_times[symbol]),
                                }
                            ]
                        },
                    }
                elif parsed.path.endswith("/v5/market/kline"):
                    symbol = query["symbol"][0]
                    interval = query["interval"][0]
                    key = (symbol, interval)
                    remaining = parent.state.transient_failures_by_stream.get(key, 0)
                    if remaining > 0:
                        parent.state.transient_failures_by_stream[key] = remaining - 1
                        payload = {"retCode": 10006, "retMsg": "Too many visits"}
                    elif parent.state.transient_kline_failures > 0:
                        parent.state.transient_kline_failures -= 1
                        payload = {"retCode": 10006, "retMsg": "Too many visits"}
                    else:
                        start = int(query["start"][0])
                        end = int(query["end"][0])
                        limit = int(query["limit"][0])
                        rows = [
                            row
                            for ts, row in parent.state.candles.get(key, {}).items()
                            if start <= ts <= end
                        ]
                        rows = sorted(
                            rows, key=lambda row: int(row[0]), reverse=True
                        )[:limit]
                        payload = {
                            "retCode": 0,
                            "retMsg": "OK",
                            "result": {"list": rows},
                        }
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

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> FakeBybitApi:
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()
