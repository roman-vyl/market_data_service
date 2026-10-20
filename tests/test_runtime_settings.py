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


def test_disabled_webhook_ignores_malformed_environment_for_its_own_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed value for a disabled feature's fields must not fail startup.

    MDS_RUNTIME_WEBHOOK_ENABLED is read first; when it is false, the other
    three variables are never parsed or validated, even if left malformed
    in the environment (e.g. a stale/unused leftover value).
    """
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_ENABLED", "false")
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS", "not-a-number")
    monkeypatch.setenv("MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY", "not-an-int")
    monkeypatch.setenv("MDS_STRATEGY_RUNTIME_BASE_URL", "malformed")

    settings = RuntimeSettings.from_environment()

    assert settings.runtime_webhook_enabled is False
    assert settings.strategy_runtime_base_url == ""
    assert settings.runtime_webhook_timeout_seconds == 2.0
    assert settings.runtime_webhook_queue_capacity == 256


def test_runtime_settings_websocket_diagnostics_defaults_and_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defaults = RuntimeSettings()
    assert defaults.websocket_ping_timeout_seconds == 60.0
    assert defaults.event_loop_lag_warn_seconds == 1.0
    monkeypatch.setenv("MDS_WS_PING_TIMEOUT_SECONDS", "90")
    monkeypatch.setenv("MDS_EVENT_LOOP_LAG_WARN_SECONDS", "2.5")
    settings = RuntimeSettings.from_environment()
    assert settings.websocket_ping_timeout_seconds == 90.0
    assert settings.event_loop_lag_warn_seconds == 2.5
    with pytest.raises(ValueError, match="websocket_ping_timeout_seconds"):
        RuntimeSettings(websocket_ping_timeout_seconds=0)


def test_stale_defaults_and_retry_schedule_validation() -> None:
    settings = RuntimeSettings()
    assert settings.stale_grace_ms == 15_000
    assert settings.stale_recovery_first_retry_seconds == 60.0
    assert settings.stale_recovery_next_retry_seconds == 360.0
    assert settings.realtime_recovery_base_seconds == 1.0
    assert settings.realtime_recovery_max_seconds == 60.0
    with pytest.raises(ValueError, match="stale_recovery_next_retry_seconds"):
        RuntimeSettings(stale_recovery_first_retry_seconds=60, stale_recovery_next_retry_seconds=30)
