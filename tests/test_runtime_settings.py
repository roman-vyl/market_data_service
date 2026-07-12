from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.runtime.settings import RuntimeSettings


def test_runtime_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_DATABASE_PATH", "/tmp/runtime.sqlite3")
    monkeypatch.setenv("MDS_HTTP_PORT", "9090")
    monkeypatch.setenv("MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM", "7")
    settings = RuntimeSettings.from_environment()
    assert settings.database_path == Path("/tmp/runtime.sqlite3")
    assert settings.http_port == 9090
    assert settings.startup_backfill_windows_per_stream == 7


def test_runtime_settings_reject_invalid_budget() -> None:
    with pytest.raises(ValueError, match="positive"):
        RuntimeSettings(startup_backfill_windows_per_stream=0)
