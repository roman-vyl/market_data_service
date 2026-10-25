"""Live open interest step of the committed-bar notification worker."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Collection
from typing import Protocol

from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.application.realtime.supervisor_types import RealtimeStreamFacts
from market_data_service.domain.identity import StreamKey
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification


class OpenInterestSupervisor(Protocol):
    def observe_open_interest(
        self, stream: StreamKey, open_time_ms: int
    ) -> tuple[RecoveryRequired, ...]: ...

    def facts(self, stream: StreamKey) -> RealtimeStreamFacts: ...


class OpenInterestPublication:
    """Bring open interest for a committed bar, then publish only a data-ready bar.

    For a stream with the open interest flag the worker calls this before the
    send: the bar's open interest is stored (off the event loop), the supervisor
    advances the open interest frontier by the same rule as for candles, and the
    bar is published only when the stream is data-ready through it. A held bar is
    published by the recovery episode that restores the stream. Streams without
    the flag pass through without any I/O.
    """

    def __init__(
        self,
        *,
        streams: Collection[StreamKey],
        append: Callable[[StreamKey, int], bool],
        supervisor: OpenInterestSupervisor,
        request_recovery: Callable[[RecoveryRequired], Awaitable[None]],
        logger: logging.Logger | None = None,
    ) -> None:
        self._streams = {
            (stream.instrument.canonical_id, stream.timeframe): stream for stream in streams
        }
        self._append = append
        self._supervisor = supervisor
        self._request_recovery = request_recovery
        self._logger = logger or logging.getLogger("market_data_service.runtime.oi_publication")

    async def prepare(self, notification: CommittedBarNotification) -> bool:
        stream = self._streams.get((notification.instrument, notification.timeframe))
        if stream is None:
            return True
        open_time_ms = notification.open_time_ms
        try:
            stored = await asyncio.to_thread(self._append, stream, open_time_ms)
        except Exception as exc:
            stored = False
            self._logger.warning(
                "live open interest fetch failed stream=%s open_time_ms=%s error=%s: %s",
                stream.canonical_id,
                open_time_ms,
                type(exc).__name__,
                exc,
            )
        if stored:
            for signal in self._supervisor.observe_open_interest(stream, open_time_ms):
                await self._request_recovery(signal)
        facts = self._supervisor.facts(stream)
        if facts.published_through(open_time_ms):
            return True
        self._logger.warning(
            "bar not published: stream not data-ready through it stream=%s open_time_ms=%s "
            "open_interest_stored=%s candle_frontier=%s open_interest_frontier=%s "
            "recovery_pending=%s",
            stream.canonical_id,
            open_time_ms,
            stored,
            facts.last_successful_open_time_ms,
            facts.last_open_interest_open_time_ms,
            facts.recovery_pending,
        )
        return False
