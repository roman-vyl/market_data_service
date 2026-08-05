"""Port for delivering committed-bar notifications to Strategy Runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CommittedBarNotification:
    """Minimal payload describing one newly committed realtime candle."""

    instrument: str
    timeframe: str
    open_time_ms: int


class CommittedBarNotifier(Protocol):
    def send(self, notification: CommittedBarNotification) -> None: ...
