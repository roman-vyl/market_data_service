"""Fetch open interest T shortly after bar T opens, so the close finds it stored."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Any

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import align_to_grid, get_timeframe


@dataclass(slots=True)
class _BarState:
    open_ms: int
    next_at_ms: int
    stored: bool = False
    in_flight: bool = False
    attempts: int = 0
    last_error: str | None = None


class OpenInterestPrefetch:
    """Store the point only; the frontier moves later, at publication (design D1).

    Attempts run off the loop under a semaphore, take no operation gate and never
    touch the WebSocket path. A queued attempt re-checks the bar right before its
    request starts (D4).
    """

    def __init__(
        self,
        *,
        streams: Collection[StreamKey],
        append: Callable[[StreamKey, int], bool],
        facts: Callable[[StreamKey], Any],
        admitted: Callable[[StreamKey], bool],
        now_ms: Callable[[], int],
        delay_seconds: float,
        retry_seconds: float,
        concurrency: int,
        tick_seconds: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self._streams = tuple(streams)
        self._append = append
        self._facts = facts
        self._admitted = admitted
        self._now_ms = now_ms
        self._delay_ms = int(delay_seconds * 1000)
        self._retry_ms = int(retry_seconds * 1000)
        self._tick_seconds = tick_seconds
        self._slots = asyncio.Semaphore(concurrency)
        self._states: dict[StreamKey, _BarState] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._logger = logger or logging.getLogger("market_data_service.runtime.oi_prefetch")

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                self.tick()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._tick_seconds)
                except TimeoutError:
                    continue
        finally:
            await self.settle()

    def tick(self) -> None:
        """Start the attempts that are due now (one per stream at a time)."""

        now = self._now_ms()
        for stream in self._streams:
            step_ms = get_timeframe(stream.timeframe).duration_ms
            open_ms = align_to_grid(now, step_ms)
            state = self._state_for(stream, open_ms)
            if state.stored or state.in_flight or now < state.next_at_ms:
                continue
            if not self._eligible(stream):
                continue
            state.in_flight = True
            state.next_at_ms = now + self._retry_ms
            task = asyncio.create_task(self._attempt(stream, state, step_ms))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def settle(self) -> None:
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

    def _state_for(self, stream: StreamKey, open_ms: int) -> _BarState:
        state = self._states.get(stream)
        if state is not None and state.open_ms == open_ms:
            return state
        if state is not None and not state.stored and state.attempts:
            self._logger.warning(
                "open interest not stored before the bar closed stream=%s open_time_ms=%s "
                "attempts=%s last_error=%s",
                stream.canonical_id,
                state.open_ms,
                state.attempts,
                state.last_error,
            )
        state = _BarState(open_ms=open_ms, next_at_ms=open_ms + self._delay_ms)
        self._states[stream] = state
        return state

    def _eligible(self, stream: StreamKey) -> bool:
        if not self._admitted(stream):
            return False
        facts = self._facts(stream)
        return bool(facts.subscription_active) and not facts.recovery_pending

    async def _attempt(self, stream: StreamKey, state: _BarState, step_ms: int) -> None:
        try:
            async with self._slots:
                # The slot may have freed late: the bar belongs to publication once closed.
                if (
                    state.stored
                    or self._now_ms() >= state.open_ms + step_ms
                    or not self._eligible(stream)
                ):
                    return
                state.attempts += 1
                try:
                    stored = await asyncio.to_thread(self._append, stream, state.open_ms)
                except Exception as exc:
                    state.last_error = f"{type(exc).__name__}: {exc}"
                    return
                state.last_error = None if stored else "not published yet"
                if stored:
                    state.stored = True
                    self._logger.info(
                        "open interest prefetched stream=%s open_time_ms=%s "
                        "seconds_after_open=%.1f attempts=%s",
                        stream.canonical_id,
                        state.open_ms,
                        (self._now_ms() - state.open_ms) / 1000,
                        state.attempts,
                    )
        finally:
            state.in_flight = False
