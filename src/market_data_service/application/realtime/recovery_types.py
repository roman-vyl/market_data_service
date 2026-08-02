"""Contracts for bounded realtime historical recovery."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.application.repair_types import (
    RepairProgressMarker,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
)
from market_data_service.domain.continuity import ContinuityReport
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


class RecoveryClassification(StrEnum):
    RESTORED = "restored"
    INCOMPLETE = "incomplete"
    RECOVERABLE_FAILURE = "recoverable_failure"
    FATAL_FAILURE = "fatal_failure"


@dataclass(frozen=True, slots=True)
class RealtimeRecoveryRequest:
    signal: RecoveryRequired
    max_windows: int
    recovery_window: TimeWindow | None = None

    def __post_init__(self) -> None:
        if self.max_windows <= 0:
            raise ValueError("max_windows must be positive")


@dataclass(frozen=True, slots=True)
class RealtimeRecoveryResult:
    stream: StreamKey
    classification: RecoveryClassification
    recovery_window: TimeWindow | None
    audit: ContinuityReport | None = None
    repair: RepairStreamGapsResult | None = None
    post_audit: ContinuityReport | None = None
    next_recovery_window: TimeWindow | None = None
    restored_through_open_time_ms: int | None = None
    error_code: str | None = None
    error_detail: str | None = None

    @property
    def restored(self) -> bool:
        return self.classification is RecoveryClassification.RESTORED

    @property
    def progress_marker(self) -> RepairProgressMarker | None:
        return None if self.repair is None else self.repair.progress_marker

    @property
    def made_progress(self) -> bool:
        if self.next_recovery_window is not None:
            return True
        if self.repair is None:
            return False
        return any(
            item.committed > 0 or item.corrected > 0 for item in self.repair.window_results
        )


class StreamRepair(Protocol):
    def execute(self, request: RepairStreamGapsRequest) -> RepairStreamGapsResult: ...
