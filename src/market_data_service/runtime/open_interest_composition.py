"""Composition of the open interest track into startup, supervision and publication."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection
from typing import Any

from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.committed_bar_notification import CommittedBarNotificationWorker
from market_data_service.runtime.committed_bar_notification_factory import (
    build_committed_bar_notifier_worker,
)
from market_data_service.runtime.open_interest_publication import OpenInterestPublication
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.wiring import RuntimeWiring


def initial_open_interest_frontiers(
    wiring: RuntimeWiring, admitted: Collection[StreamKey]
) -> dict[StreamKey, int | None]:
    """Open interest frontier of every flagged stream; absent keys mean no flag.

    A stream admitted by the startup pass had its track proven by that pass, so
    its frontier is the end of that proof; any other flagged stream stays
    unproven until its own hand-off.
    """

    return {
        stream: wiring.open_interest_latest(stream) if stream in admitted else None
        for stream in wiring.config.open_interest_streams
    }


def open_interest_notification_worker(
    wiring: RuntimeWiring,
    settings: RuntimeSettings,
    supervisor: RealtimeSupervisor,
    request_recovery: Callable[[RecoveryRequired], Awaitable[None]],
) -> CommittedBarNotificationWorker | None:
    """The notification worker, with the live open interest step for flagged streams."""

    streams = wiring.config.open_interest_streams
    publication = (
        OpenInterestPublication(
            streams=streams,
            append=wiring.open_interest_live(settings.oi_live_timeout_seconds).execute,
            supervisor=supervisor,
            request_recovery=request_recovery,
        )
        if streams
        else None
    )
    return build_committed_bar_notifier_worker(
        settings, wiring.config, prepare=None if publication is None else publication.prepare
    )


def open_interest_startup(wiring: RuntimeWiring, settings: RuntimeSettings) -> dict[str, Any]:
    """Startup reconciliation proves the open interest track of flagged streams too."""

    return {
        "open_interest": wiring.open_interest_reconciler(settings.oi_track_windows_per_pass),
        "open_interest_streams": frozenset(wiring.config.open_interest_streams),
    }


def admit_with_open_interest(
    wiring: RuntimeWiring,
    config: ValidatedMarketConfig,
    realtime: RuntimeRealtimeCoordinator,
) -> Callable[[StreamKey], Awaitable[None]]:
    """Hand-off after startup reconciliation proved both tracks of a stream."""

    flagged = frozenset(config.open_interest_streams)

    async def handoff(stream: StreamKey) -> None:
        if stream in flagged:
            latest = wiring.open_interest_latest(stream)
            if latest is not None:
                realtime.supervisor.synchronize_open_interest_open_time(stream, latest)
        await realtime.admit(stream)

    return handoff
