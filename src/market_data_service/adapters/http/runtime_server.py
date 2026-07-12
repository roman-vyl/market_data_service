"""Minimal standard-library health/readiness HTTP adapter."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from market_data_service.runtime.status import RuntimeStatusStore


class RuntimeHttpServer:
    def __init__(self, host: str, port: int, status: RuntimeStatusStore) -> None:
        self._status = status
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health":
                    document = outer._status.health_document()
                    outer._write(self, 200 if outer._status.healthy else 503, document)
                    return
                if self.path == "/readiness":
                    document = outer._status.readiness_document()
                    outer._write(self, 200 if outer._status.ready else 503, document)
                    return
                outer._write(self, 404, {"error": "not_found"})

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
