from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.runtime.settings import RuntimeSettings


def test_runtime_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_DATABASE_PATH", "/tmp/runtime.sqlite3")
    monkeypatch.setenv("MDS_HTTP_PORT", "9090")
    monkeypatch.setenv("MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM", "7")
    monkeypatch.setenv("MDS_REALTIME_RECOVERY_BASE_SECONDS", "2.5")
    monkeypatch.setenv("MDS_REALTIME_STALE_CHECK_SECONDS", "0.25")
    settings = RuntimeSettings.from_environment()
    assert settings.database_path == Path("/tmp/runtime.sqlite3")
    assert settings.http_port == 9090
    assert settings.startup_backfill_windows_per_stream == 7
    assert settings.realtime_recovery_base_seconds == 2.5
    assert settings.realtime_stale_check_seconds == 0.25


def test_runtime_settings_reject_invalid_budget() -> None:
    with pytest.raises(ValueError, match="positive"):
        RuntimeSettings(startup_backfill_windows_per_stream=0)


def test_runtime_settings_reject_invalid_realtime_policy() -> None:
    with pytest.raises(ValueError, match="realtime_recovery_max_seconds"):
        RuntimeSettings(
            realtime_recovery_base_seconds=2,
            realtime_recovery_max_seconds=1,
        )
    with pytest.raises(ValueError, match="realtime_stale_check_seconds"):
        RuntimeSettings(realtime_stale_check_seconds=0)
