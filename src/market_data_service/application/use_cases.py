"""Named application use-case boundaries."""

from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
    UnknownStreamError,
)


class ResolveEarliestAvailableCandle:
    """Resolve and persist a stream's observed historical lower bound."""


class BootstrapFullMinuteHistory:
    """Populate full available 1m history through finite sequential runs."""


class RepairStreamGaps:
    """Repair planned gaps through REST and mandatory postflight audit."""


class EvaluateStreamReadiness:
    """Project durable per-stream state into readiness."""


__all__ = [
    "AuditStreamContinuity",
    "AuditStreamContinuityRequest",
    "BootstrapFullMinuteHistory",
    "EvaluateStreamReadiness",
    "RepairStreamGaps",
    "ResolveEarliestAvailableCandle",
    "UnknownStreamError",
]
