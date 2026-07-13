"""Map application errors to stable HTTP error envelopes."""

from __future__ import annotations

from market_data_service.application.consumer_read.errors import (
    ConfiguredStreamNotFound,
    ConsumerReadError,
    ContinuityInvariantBroken,
    InvalidRange,
    RangeNotAligned,
    RangeOutOfBounds,
    StreamNotReady,
)


def map_exception(exc: Exception) -> tuple[int, dict[str, object]]:
    if isinstance(exc, ConfiguredStreamNotFound):
        status = 404
    elif isinstance(exc, StreamNotReady):
        status = 409
    elif isinstance(exc, (InvalidRange, RangeNotAligned, RangeOutOfBounds)):
        status = 422
    elif isinstance(exc, ContinuityInvariantBroken):
        status = 500
    else:
        status = 500
    code = (
        exc.code
        if isinstance(exc, (ConsumerReadError, ContinuityInvariantBroken))
        else "internal_error"
    )
    return status, {"error": code, "detail": str(exc)}
