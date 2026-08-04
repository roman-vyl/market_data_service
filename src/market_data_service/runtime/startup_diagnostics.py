"""Structured console diagnostics for initial stream reconciliation."""

from __future__ import annotations

import logging

from market_data_service.runtime.startup_types import StartupStreamOutcome


class StartupDiagnostics:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def record(self, outcome: StartupStreamOutcome) -> None:
        window = outcome.window
        self._logger.info(
            "startup stream=%s classification=%s start_ms=%s end_ms=%s "
            "remaining_gaps=%s attempted=%s completed=%s committed=%s "
            "duplicates=%s corrected=%s rejected=%s unexpected=%s error=%s detail=%s",
            outcome.stream.canonical_id,
            outcome.classification.value,
            None if window is None else window.start_time_ms,
            None if window is None else window.end_time_ms,
            None if outcome.audit is None else len(outcome.audit.gaps),
            outcome.counts.attempted_windows,
            outcome.counts.completed_windows,
            outcome.counts.committed,
            outcome.counts.duplicates,
            outcome.counts.corrected,
            outcome.counts.rejected,
            outcome.counts.unexpected,
            outcome.error_code,
            outcome.error_detail,
        )
