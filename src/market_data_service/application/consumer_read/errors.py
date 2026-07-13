"""Application errors for consumer candle reads."""

from __future__ import annotations


class ConsumerReadError(ValueError):
    code = "consumer_read_error"


class ConfiguredStreamNotFound(ConsumerReadError):
    code = "configured_stream_not_found"


class StreamNotReady(ConsumerReadError):
    code = "stream_not_ready"


class InvalidRange(ConsumerReadError):
    code = "invalid_range"


class RangeNotAligned(ConsumerReadError):
    code = "range_not_aligned"


class RangeOutOfBounds(ConsumerReadError):
    code = "range_out_of_bounds"


class ContinuityInvariantBroken(RuntimeError):
    code = "continuity_invariant_broken"
