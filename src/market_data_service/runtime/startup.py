"""Deterministic first bounded reconciliation pass for configured streams."""

from __future__ import annotations

from collections.abc import Sequence

from market_data_service.domain.identity import StreamKey
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.startup_types import (
    ReconciliationWindow,
    StartupStreamOutcome,
)


class StartupCoordinator:
    def __init__(self, reconciler: HistoricalStreamReconciler) -> None:
        self._reconciler = reconciler

    def execute(self, streams: Sequence[StreamKey]) -> tuple[StartupStreamOutcome, ...]:
        return tuple(self.execute_stream(stream) for stream in streams)

    def execute_stream(
        self,
        stream: StreamKey,
        window: ReconciliationWindow | None = None,
    ) -> StartupStreamOutcome:
        return self._reconciler.execute(stream, window)
