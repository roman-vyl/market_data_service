"""Per-stream state of one non-terminal realtime recovery."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.application.realtime.events import RecoveryReason, RecoveryRequired
from market_data_service.application.repair_types import RepairProgressMarker
from market_data_service.domain.windows import TimeWindow


def is_stale_signal(signal: RecoveryRequired) -> bool:
    return signal.reason is RecoveryReason.STALE


@dataclass(slots=True)
class PendingRecovery:
    signal: RecoveryRequired
    failures: int = 0
    no_progress_attempts: int = 0
    stale_failures: int = 0
    due_at: float = 0.0
    recovery_window: TimeWindow | None = None
    progress_marker: RepairProgressMarker | None = None
    executing: bool = False

    def can_be_preempted_by(self, signal: RecoveryRequired) -> bool:
        """Only a stale recovery that waits for its retry yields to another reason."""

        return not self.executing and is_stale_signal(self.signal) and not is_stale_signal(signal)

    def preempt(self, signal: RecoveryRequired) -> None:
        """Start over for the new signal: nothing is inherited from the stale attempt."""

        self.signal = signal
        self.failures = 0
        self.no_progress_attempts = 0
        self.stale_failures = 0
        self.due_at = 0.0
        self.recovery_window = None
        self.progress_marker = None
