"""Composition-time construction of the optional committed-bar notifier."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from market_data_service.adapters.http.committed_bar_notifier import HttpCommittedBarNotifier
from market_data_service.config import ValidatedMarketConfig
from market_data_service.ports.committed_bar_notifier import (
    CommittedBarNotification,
    CommittedBarNotifier,
)
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)
from market_data_service.runtime.settings import RuntimeSettings


class _WebhookDisabled:
    """Open interest streams still need the worker's live step when the webhook is off."""

    def send(self, notification: CommittedBarNotification) -> None:
        logging.getLogger("market_data_service.runtime.committed_bar_notification").debug(
            "runtime webhook disabled; not sending instrument=%s open_time_ms=%s",
            notification.instrument,
            notification.open_time_ms,
        )


def build_committed_bar_notifier_worker(
    settings: RuntimeSettings,
    config: ValidatedMarketConfig,
    *,
    prepare: Callable[[CommittedBarNotification], Awaitable[bool]] | None = None,
) -> CommittedBarNotificationWorker | None:
    if not settings.runtime_webhook_enabled:
        if not config.open_interest_streams:
            return None
        disabled: CommittedBarNotifier = _WebhookDisabled()
        return CommittedBarNotificationWorker(
            disabled, settings.runtime_webhook_queue_capacity, prepare=prepare
        )
    if settings.runtime_webhook_queue_capacity < len(config.enabled_streams):
        raise ValueError(
            "MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY must be at least the number of enabled streams"
        )
    notifier = HttpCommittedBarNotifier(
        settings.strategy_runtime_base_url,
        settings.runtime_webhook_timeout_seconds,
    )
    return CommittedBarNotificationWorker(
        notifier, settings.runtime_webhook_queue_capacity, prepare=prepare
    )
