"""Named application use-case boundaries."""



class ResolveEarliestAvailableCandle:
    """Resolve and persist a stream's observed historical lower bound."""


class BootstrapFullMinuteHistory:
    """Populate full available 1m history through finite sequential runs."""


class AuditStreamContinuity:
    """Find internal and trailing canonical gaps for one stream."""


class RepairStreamGaps:
    """Repair planned gaps through REST and mandatory postflight audit."""


class EvaluateStreamReadiness:
    """Project durable per-stream state into readiness."""
