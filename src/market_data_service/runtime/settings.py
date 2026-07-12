"""Validated runtime settings with CLI/environment/default precedence."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    database_path: Path = Path("data/market.sqlite3")
    markets_config_path: Path = Path("config/markets.toml")
    http_host: str = "127.0.0.1"
    http_port: int = 8080
    rest_base_url: str = "https://api.bybit.com"
    websocket_url: str = "wss://stream.bybit.com/v5/public/linear"
    startup_backfill_windows_per_stream: int = 2
    startup_repair_windows_per_stream: int = 2
    reconnect_max_attempts: int = 3
    reconnect_delay_seconds: float = 1.0
    stale_intervals: int = 2
    stale_grace_ms: int = 5_000
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not self.http_host.strip():
            raise ValueError("http_host must not be empty")
        if not 1 <= self.http_port <= 65_535:
            raise ValueError("http_port must be in range 1..65535")
        for name in (
            "startup_backfill_windows_per_stream",
            "startup_repair_windows_per_stream",
            "reconnect_max_attempts",
            "stale_intervals",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        if self.stale_grace_ms < 0:
            raise ValueError("stale_grace_ms must be non-negative")
        if self.log_level.upper() not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("unsupported log_level")

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        env = os.environ
        return cls(
            database_path=Path(env.get("MDS_DATABASE_PATH", "data/market.sqlite3")),
            markets_config_path=Path(env.get("MDS_MARKETS_CONFIG_PATH", "config/markets.toml")),
            http_host=env.get("MDS_HTTP_HOST", "127.0.0.1"),
            http_port=int(env.get("MDS_HTTP_PORT", "8080")),
            rest_base_url=env.get("MDS_REST_BASE_URL", "https://api.bybit.com"),
            websocket_url=env.get(
                "MDS_WEBSOCKET_URL", "wss://stream.bybit.com/v5/public/linear"
            ),
            startup_backfill_windows_per_stream=int(
                env.get("MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM", "2")
            ),
            startup_repair_windows_per_stream=int(
                env.get("MDS_STARTUP_REPAIR_WINDOWS_PER_STREAM", "2")
            ),
            reconnect_max_attempts=int(env.get("MDS_RECONNECT_MAX_ATTEMPTS", "3")),
            reconnect_delay_seconds=float(env.get("MDS_RECONNECT_DELAY_SECONDS", "1.0")),
            stale_intervals=int(env.get("MDS_STALE_INTERVALS", "2")),
            stale_grace_ms=int(env.get("MDS_STALE_GRACE_MS", "5000")),
            log_level=env.get("MDS_LOG_LEVEL", "INFO").upper(),
        )
