from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import urlopen

from market_data_service.adapters.http import RuntimeHttpServer
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.runtime.status import RuntimeStatusStore


def _get(url: str) -> tuple[int, dict[str, object]]:
    try:
        response = urlopen(url, timeout=2)
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())
    return response.status, json.loads(response.read())


def test_health_and_readiness_status_codes() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    status = RuntimeStatusStore((stream,))
    server = RuntimeHttpServer("127.0.0.1", 0, status)
    server.start()
    host, port = server.address
    try:
        code, health = _get(f"http://{host}:{port}/health")
        assert code == 503
        assert health["status"] == "unhealthy"
        status.mark_healthy()
        code, health = _get(f"http://{host}:{port}/health")
        assert code == 200
        assert health["status"] == "healthy"
        code, readiness = _get(f"http://{host}:{port}/readiness")
        assert code == 503
        assert readiness["ready"] is False
    finally:
        server.close()
