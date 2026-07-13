from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[1] / "src" / "market_data_service"


def test_application_consumer_read_does_not_import_adapters() -> None:
    text = "\n".join(p.read_text() for p in (ROOT / "application" / "consumer_read").glob("*.py"))
    assert "market_data_service.adapters" not in text


def test_sqlite_consumer_reader_does_not_import_http() -> None:
    text = (ROOT / "adapters" / "sqlite" / "consumer_candle_reader.py").read_text()
    assert "adapters.http" not in text


def test_runtime_server_remains_small_transport_composition() -> None:
    path = ROOT / "adapters" / "http" / "runtime_server.py"
    assert len(path.read_text().splitlines()) < 100
    assert "sqlite" not in path.read_text().lower()
