"""Open interest as the second data track of a realtime recovery episode."""

from __future__ import annotations

from collections.abc import Collection

from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryResult,
    RecoveryClassification,
    StreamRepair,
)
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
)
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


class OpenInterestRecoveryTrack:
    """Repair open interest over the episode window with the candle gap repair.

    The window comes from the recovery signal, whose start is never later than
    the bar after proven continuity of both tracks (see the supervisor), so a
    hole after the last proven bar is always inside the audited range.
    """

    def __init__(self, *, streams: Collection[StreamKey], repair: StreamRepair) -> None:
        self._streams = frozenset(streams)
        self._repair = repair

    def required(self, stream: StreamKey) -> bool:
        return stream in self._streams

    def repair(
        self, stream: StreamKey, window: TimeWindow, max_windows: int
    ) -> RepairStreamGapsResult:
        return self._repair.execute(
            RepairStreamGapsRequest(
                stream=stream,
                start_time_ms=window.start_ms,
                end_time_ms=window.end_ms,
                max_windows=max_windows,
            )
        )

    @staticmethod
    def unfinished(
        stream: StreamKey,
        window: TimeWindow,
        repair: RepairStreamGapsResult,
        open_interest_repair: RepairStreamGapsResult,
    ) -> RealtimeRecoveryResult:
        """The episode is not restored while the open interest track is not continuous."""

        if open_interest_repair.status is RepairStatus.FAILED:
            classification = (
                RecoveryClassification.RECOVERABLE_FAILURE
                if open_interest_repair.failure_disposition == "recoverable"
                else RecoveryClassification.FATAL_FAILURE
            )
            return RealtimeRecoveryResult(
                stream=stream,
                classification=classification,
                recovery_window=window,
                audit=repair.pre_repair_audit,
                repair=repair,
                open_interest_repair=open_interest_repair,
                error_code=open_interest_repair.error_code,
                error_detail=open_interest_repair.error_detail,
            )
        return RealtimeRecoveryResult(
            stream=stream,
            classification=RecoveryClassification.INCOMPLETE,
            recovery_window=window,
            audit=repair.pre_repair_audit,
            repair=repair,
            open_interest_repair=open_interest_repair,
            post_audit=open_interest_repair.post_repair_audit,
            error_code="post_recovery_open_interest_incomplete",
            error_detail="post-recovery open interest audit still reports gaps",
        )
