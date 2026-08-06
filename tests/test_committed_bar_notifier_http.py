from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from market_data_service.adapters.http.committed_bar_notifier import (
    CommittedBarDeliveryError,
    HttpCommittedBarNotifier,
)
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification


def _notification() -> CommittedBarNotification:
    return CommittedBarNotification(instrument="BTCUSDT.P", timeframe="1m", open_time_ms=0)


@contextmanager
def _server(
    respond: Callable[[BaseHTTPRequestHandler], None],
) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            respond(self)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _accept(handler: BaseHTTPRequestHandler) -> None:
    length = int(handler.headers.get("Content-Length", "0"))
    body = json.loads(handler.rfile.read(length).decode("utf-8"))
    assert body == {"instrument": "BTCUSDT.P", "timeframe": "1m", "open_time_ms": 0}
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json")
    handler.end_headers()
    handler.wfile.write(json.dumps({"status": "accepted"}).encode("utf-8"))


def test_successful_delivery_posts_exact_payload_and_path() -> None:
    calls: list[str] = []

    def respond(handler: BaseHTTPRequestHandler) -> None:
        calls.append(handler.path)
        _accept(handler)

    with _server(respond) as base_url:
        notifier = HttpCommittedBarNotifier(base_url, timeout_seconds=2.0)
        notifier.send(_notification())

    assert calls == ["/v1/webhooks/closed-bar"]


def test_non_200_status_raises_delivery_error() -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"status": "not_ready"}).encode("utf-8"))

    with _server(respond) as base_url:
        notifier = HttpCommittedBarNotifier(base_url, timeout_seconds=2.0)
        with pytest.raises(CommittedBarDeliveryError):
            notifier.send(_notification())


def test_malformed_success_body_raises_delivery_error() -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"status": "unexpected"}).encode("utf-8"))

    with _server(respond) as base_url:
        notifier = HttpCommittedBarNotifier(base_url, timeout_seconds=2.0)
        with pytest.raises(CommittedBarDeliveryError):
            notifier.send(_notification())


def test_non_json_success_body_raises_delivery_error() -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.end_headers()
        handler.wfile.write(b"not json")

    with _server(respond) as base_url:
        notifier = HttpCommittedBarNotifier(base_url, timeout_seconds=2.0)
        with pytest.raises(CommittedBarDeliveryError):
            notifier.send(_notification())


def test_connection_refused_raises_delivery_error() -> None:
    notifier = HttpCommittedBarNotifier("http://127.0.0.1:1", timeout_seconds=1.0)
    with pytest.raises(CommittedBarDeliveryError):
        notifier.send(_notification())


def test_timeout_raises_delivery_error() -> None:
    ready = threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        ready.set()
        time.sleep(2)
        _accept(handler)

    with _server(respond) as base_url:
        notifier = HttpCommittedBarNotifier(base_url, timeout_seconds=0.05)
        with pytest.raises(CommittedBarDeliveryError):
            notifier.send(_notification())
