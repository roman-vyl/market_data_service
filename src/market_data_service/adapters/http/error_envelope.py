"""Map application/domain errors to the shared public read/planning HTTP envelope.

Every public read/planning endpoint (consumer_read, historical_read,
history_planning) reports errors as {"error": <code>, "detail": <message>}
through this single function, so the wire contract is identical regardless
of which handler produced the failure.
"""

from __future__ import annotations

from market_data_service.application.audit_continuity import UnknownStreamError
from market_data_service.application.consumer_read.errors import (
    ConfiguredStreamNotFound,
    ContinuityInvariantBroken,
    CoverageStale,
    InvalidRange,
    RangeNotAligned,
    RangeOutOfBounds,
    StreamNotReady,
)


def map_exception(exc: Exception) -> tuple[int, dict[str, object]]:
    if isinstance(exc, LookupError):
        return 404, {"error": "not_found", "detail": str(exc)}
    if isinstance(exc, UnknownStreamError):
        return 404, {"error": "configured_stream_not_found", "detail": str(exc)}
    if isinstance(exc, ConfiguredStreamNotFound):
        return 404, {"error": exc.code, "detail": str(exc)}
    if isinstance(exc, (StreamNotReady, CoverageStale)):
        return 409, {"error": exc.code, "detail": str(exc)}
    if isinstance(exc, (InvalidRange, RangeNotAligned, RangeOutOfBounds)):
        return 422, {"error": exc.code, "detail": str(exc)}
    if isinstance(exc, ContinuityInvariantBroken):
        return 500, {"error": exc.code, "detail": str(exc)}
    if isinstance(exc, ValueError):
        return 422, {"error": "invalid_request", "detail": str(exc)}
    return 500, {"error": "internal_error", "detail": str(exc)}
