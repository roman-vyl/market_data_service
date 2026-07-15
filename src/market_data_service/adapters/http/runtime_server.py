"""Minimal standard-library health/readiness HTTP adapter."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from market_data_service.adapters.http.consumer_read import ConsumerReadHttpHandler
from market_data_service.adapters.http.historical_read import HistoricalReadHttpHandler
from market_data_service.adapters.http.history_planning import HistoryPlanningHttpHandler
from market_data_service.adapters.http.request_router import (
    RuntimeRequestRouter,
    RuntimeStatusView,
)


class RuntimeHttpServer:
    def __init__(
        self,
        host: str,
        port: int,
        status: RuntimeStatusView,
        consumer_read: ConsumerReadHttpHandler | None = None,
        history_planning: HistoryPlanningHttpHandler | None = None,
        historical_read: HistoricalReadHttpHandler | None = None,
    ) -> None:
        router = RuntimeRequestRouter(status, consumer_read, history_planning, historical_read)
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                outer._write(self, *router.get(self.path))

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                outer._write(self, *router.post(self.path, self.rfile.read(length)))

            def log_message(self, format: str, *args: object) -> None:
                return

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        address = self._server.server_address
        return str(address[0]), int(address[1])

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()

    @staticmethod
    def _write(
        handler: BaseHTTPRequestHandler,
        status: int,
        document: dict[str, object],
    ) -> None:
        payload = json.dumps(document, sort_keys=True).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)
