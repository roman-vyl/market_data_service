from __future__ import annotations

import pytest

from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.identity import InstrumentKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)
from market_data_service.runtime.committed_bar_notification_factory import (
    build_committed_bar_notifier_worker,
)
from market_data_service.runtime.settings import RuntimeSettings


def _config(*tickers: str, timeframes: tuple[str, ...] = ("1m",)) -> ValidatedMarketConfig:
    return ValidatedMarketConfig(
        1,
        MarketSourceConfig("bybit", "linear"),
        tuple(
            InstrumentCoverage(
                InstrumentKey(ticker),
                ticker.removesuffix(".P"),
                True,
                timeframes,
                HistoryPolicy.FULL_AVAILABLE,
            )
            for ticker in tickers
        ),
    )


def test_disabled_webhook_builds_no_worker() -> None:
    settings = RuntimeSettings(runtime_webhook_enabled=False)
    worker = build_committed_bar_notifier_worker(settings, _config("BTCUSDT.P"))
    assert worker is None


def test_enabled_webhook_with_sufficient_capacity_builds_one_worker() -> None:
    settings = RuntimeSettings(
        runtime_webhook_enabled=True,
        strategy_runtime_base_url="http://localhost:8093",
        runtime_webhook_queue_capacity=2,
    )
    worker = build_committed_bar_notifier_worker(settings, _config("BTCUSDT.P", "ETHUSDT.P"))
    assert isinstance(worker, CommittedBarNotificationWorker)


def test_enabled_webhook_with_capacity_smaller_than_enabled_streams_fails_fast() -> None:
    settings = RuntimeSettings(
        runtime_webhook_enabled=True,
        strategy_runtime_base_url="http://localhost:8093",
        runtime_webhook_queue_capacity=1,
    )
    with pytest.raises(ValueError, match="MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY"):
        build_committed_bar_notifier_worker(settings, _config("BTCUSDT.P", "ETHUSDT.P"))


def test_enabled_webhook_with_capacity_equal_to_enabled_streams_succeeds() -> None:
    settings = RuntimeSettings(
        runtime_webhook_enabled=True,
        strategy_runtime_base_url="http://localhost:8093",
        runtime_webhook_queue_capacity=2,
    )
    worker = build_committed_bar_notifier_worker(settings, _config("BTCUSDT.P", "ETHUSDT.P"))
    assert isinstance(worker, CommittedBarNotificationWorker)
