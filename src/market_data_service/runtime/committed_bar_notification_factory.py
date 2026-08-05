"""Composition-time construction of the optional committed-bar notifier."""

from __future__ import annotations

from market_data_service.adapters.http.committed_bar_notifier import HttpCommittedBarNotifier
from market_data_service.config import ValidatedMarketConfig
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)
from market_data_service.runtime.settings import RuntimeSettings


def build_committed_bar_notifier_worker(
    settings: RuntimeSettings,
    config: ValidatedMarketConfig,
) -> CommittedBarNotificationWorker | None:
    if not settings.runtime_webhook_enabled:
        return None
    if settings.runtime_webhook_queue_capacity < len(config.enabled_streams):
        raise ValueError(
            "MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY must be at least the number of "
            "enabled streams"
        )
    notifier = HttpCommittedBarNotifier(
        settings.strategy_runtime_base_url,
        settings.runtime_webhook_timeout_seconds,
    )
    return CommittedBarNotificationWorker(notifier, settings.runtime_webhook_queue_capacity)
