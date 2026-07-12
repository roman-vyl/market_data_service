"""Contracts for bounded realtime historical recovery."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from market_data_service.application.audit_continuity import AuditStreamContinuityRequest
from market_data_service.application.backfill_types import (
    BackfillStreamRequest,
    BackfillStreamResult,
)
from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.application.repair_types import (
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
    max_backfill_windows: int
    max_repair_windows: int

    def __post_init__(self) -> None:
        if self.max_backfill_windows <= 0:
            raise ValueError("max_backfill_windows must be positive")
        if self.max_repair_windows <= 0:
            raise ValueError("max_repair_windows must be positive")


@dataclass(frozen=True, slots=True)
class RealtimeRecoveryResult:
    stream: StreamKey
    classification: RecoveryClassification
    recovery_window: TimeWindow | None
    backfill: BackfillStreamResult | None = None
    audit: ContinuityReport | None = None
    repair: RepairStreamGapsResult | None = None
    post_audit: ContinuityReport | None = None
    restored_through_open_time_ms: int | None = None
    error_code: str | None = None
    error_detail: str | None = None

    @property
    def restored(self) -> bool:
        return self.classification is RecoveryClassification.RESTORED


class StreamBackfill(Protocol):
    def execute(self, request: BackfillStreamRequest) -> BackfillStreamResult: ...


class StreamAudit(Protocol):
    def execute(self, request: AuditStreamContinuityRequest) -> ContinuityReport: ...


class StreamRepair(Protocol):
    def execute(self, request: RepairStreamGapsRequest) -> RepairStreamGapsResult: ...
