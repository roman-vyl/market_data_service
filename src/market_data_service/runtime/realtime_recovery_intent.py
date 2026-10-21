"""Per-stream recovery intent: what must be reconciled, not which event asked for it."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.application.realtime.events import RecoveryReason, RecoveryRequired
from market_data_service.application.repair_types import RepairProgressMarker
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


def is_background(signal: RecoveryRequired) -> bool:
    """A missing expected bar is background work; every other trigger is urgent."""

    return signal.reason is RecoveryReason.STALE


@dataclass(slots=True)
class RecoveryIntent:
    """The single recovery wanted for one stream.

    ``revision`` moves only when the requirement really changes. The window and
    progress marker are the execution cursor of the current revision, not intent.
    """

    stream: StreamKey
    reason: RecoveryReason
    detected_at_ms: int
    earliest_open_ms: int | None = None
    urgent: bool = False
    revision: int = 0
    due_at: float = 0.0
    attempts_without_progress: int = 0
    recovery_window: TimeWindow | None = None
    progress_marker: RepairProgressMarker | None = None
    # Sticky for the whole episode: Runtime may have refused a bar while the stream was
    # not READY, or REST committed bars Runtime never heard about. Ends with the episode.
    retry_needed: bool = False
    notify_open_ms: int | None = None

    @classmethod
    def from_signal(cls, signal: RecoveryRequired) -> RecoveryIntent:
        return cls(
            stream=signal.stream,
            reason=signal.reason,
            detected_at_ms=signal.detected_at_ms,
            earliest_open_ms=signal.suspected_start_time_ms,
            urgent=not is_background(signal),
        )

    def absorb(self, signal: RecoveryRequired) -> bool:
        """Merge a signal; return True only if the requirement really changed."""

        changed = False
        hint = signal.suspected_start_time_ms
        if hint is not None and (self.earliest_open_ms is None or hint < self.earliest_open_ms):
            self.earliest_open_ms = hint
            changed = True
        if not is_background(signal) and not self.urgent:
            self.urgent = True
            self.reason = signal.reason
            changed = True
        if changed:
            self.revision += 1
            self.restart()
        return changed

    def restart(self) -> None:
        """A real change drops the old cursor and backoff and wants a pass now."""

        self.due_at = 0.0
        self.attempts_without_progress = 0
        self.recovery_window = None
        self.progress_marker = None

    def request_signal(self) -> RecoveryRequired:
        return RecoveryRequired(
            stream=self.stream,
            reason=self.reason,
            detected_at_ms=self.detected_at_ms,
            suspected_start_time_ms=self.earliest_open_ms,
        )
