from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.runtime.settings import RuntimeSettings


def test_runtime_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_DATABASE_PATH", "/tmp/runtime.sqlite3")
    monkeypatch.setenv("MDS_HTTP_PORT", "9090")
    monkeypatch.setenv("MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM", "7")
    monkeypatch.setenv("MDS_REALTIME_RECOVERY_WINDOWS_PER_STREAM", "3")
    monkeypatch.setenv("MDS_REALTIME_RECOVERY_BASE_SECONDS", "2.5")
    monkeypatch.setenv("MDS_REALTIME_STALE_CHECK_SECONDS", "0.25")
    settings = RuntimeSettings.from_environment()
    assert settings.database_path == Path("/tmp/runtime.sqlite3")
    assert settings.http_port == 9090
    assert settings.startup_backfill_windows_per_stream == 7
    assert settings.realtime_recovery_windows_per_stream == 3
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


def test_committed_bar_webhook_disabled_by_default() -> None:
    settings = RuntimeSettings()
    assert settings.runtime_webhook_enabled is False
    assert settings.runtime_webhook_timeout_seconds == 2.0
    assert settings.runtime_webhook_queue_capacity == 256


def test_committed_bar_webhook_disabled_allows_empty_base_url() -> None:
    RuntimeSettings(runtime_webhook_enabled=False, strategy_runtime_base_url="")


def test_committed_bar_webhook_enabled_requires_valid_base_url() -> None:
    with pytest.raises(ValueError, match="strategy_runtime_base_url"):
        RuntimeSettings(runtime_webhook_enabled=True, strategy_runtime_base_url="")
    with pytest.raises(ValueError, match="strategy_runtime_base_url"):
        RuntimeSettings(runtime_webhook_enabled=True, strategy_runtime_base_url="not-a-url")
    RuntimeSettings(
        runtime_webhook_enabled=True,
        strategy_runtime_base_url="http://localhost:8093",
    )


def test_committed_bar_webhook_enabled_uses_documented_defaults_when_absent() -> None:
    settings = RuntimeSettings(
        runtime_webhook_enabled=True,
        strategy_runtime_base_url="http://localhost:8093",
    )
    assert settings.runtime_webhook_timeout_seconds == 2.0
    assert settings.runtime_webhook_queue_capacity == 256


def test_committed_bar_webhook_enabled_rejects_invalid_explicit_timeout() -> None:
    with pytest.raises(ValueError, match="runtime_webhook_timeout_seconds"):
        RuntimeSettings(
            runtime_webhook_enabled=True,
            strategy_runtime_base_url="http://localhost:8093",
            runtime_webhook_timeout_seconds=0,
        )
    with pytest.raises(ValueError, match="runtime_webhook_timeout_seconds"):
        RuntimeSettings(
            runtime_webhook_enabled=True,
            strategy_runtime_base_url="http://localhost:8093",
            runtime_webhook_timeout_seconds=float("inf"),
        )


def test_committed_bar_webhook_enabled_rejects_invalid_explicit_capacity() -> None:
    with pytest.raises(ValueError, match="runtime_webhook_queue_capacity"):
        RuntimeSettings(
            runtime_webhook_enabled=True,
            strategy_runtime_base_url="http://localhost:8093",
            runtime_webhook_queue_capacity=0,
        )


def test_runtime_settings_read_committed_bar_webhook_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_ENABLED", "true")
    monkeypatch.setenv("MDS_STRATEGY_RUNTIME_BASE_URL", "http://localhost:8093")
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS", "5.0")
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY", "64")
    settings = RuntimeSettings.from_environment()
    assert settings.runtime_webhook_enabled is True
    assert settings.strategy_runtime_base_url == "http://localhost:8093"
    assert settings.runtime_webhook_timeout_seconds == 5.0
    assert settings.runtime_webhook_queue_capacity == 64


def test_runtime_settings_committed_bar_webhook_defaults_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MDS_RUNTIME_WEBHOOK_ENABLED", raising=False)
    monkeypatch.delenv("MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY", raising=False)
    settings = RuntimeSettings.from_environment()
    assert settings.runtime_webhook_enabled is False
    assert settings.runtime_webhook_timeout_seconds == 2.0
    assert settings.runtime_webhook_queue_capacity == 256
