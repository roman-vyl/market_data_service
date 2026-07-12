"""Public domain contracts and pure functions."""

from market_data_service.domain.backfill import BackfillBudget, BackfillRequest, BackfillSelection
from market_data_service.domain.candle_comparison import classify_against_existing
from market_data_service.domain.candle_validation import (
    CandleValidationCode,
    CandleValidationIssue,
    validate_observed_candle,
)
from market_data_service.domain.candles import CanonicalCandle, ObservationSource, ObservedCandle
from market_data_service.domain.classification import IngestionClassification
from market_data_service.domain.continuity import (
    ContinuityReport,
    build_continuity_report,
)
from market_data_service.domain.decimal_values import (
    DecimalInput,
    InvalidDecimalValue,
    decimal_to_canonical_text,
    parse_canonical_decimal_text,
    parse_decimal,
)
from market_data_service.domain.gaps import Gap, find_gaps, iter_fetch_windows
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import (
    ExchangeInstrumentSpecification,
    HistoryPolicy,
    InstrumentCoverage,
    InstrumentMetadata,
)
from market_data_service.domain.readiness import (
    StreamReadiness,
    project_stream_readiness,
    strict_aggregate_readiness,
)
from market_data_service.domain.stream_state import (
    InvalidStreamTransition,
    StreamLifecycleState,
    StreamStateSnapshot,
    can_transition,
    transition_stream_state,
)
from market_data_service.domain.timeframes import (
    TimeframeSpec,
    align_to_grid,
    ceil_to_grid,
    get_timeframe,
    last_closed_open_time_ms,
)
from market_data_service.domain.windows import TimeWindow

__all__ = [
    "BackfillBudget",
    "BackfillRequest",
    "BackfillSelection",
    "CandleValidationCode",
    "CandleValidationIssue",
    "CanonicalCandle",
    "ContinuityReport",
    "DecimalInput",
    "ExchangeInstrumentSpecification",
    "Gap",
    "HistoryPolicy",
    "IngestionClassification",
    "InstrumentCoverage",
    "InstrumentKey",
    "InstrumentMetadata",
    "InvalidDecimalValue",
    "InvalidStreamTransition",
    "ObservationSource",
    "ObservedCandle",
    "StreamKey",
    "StreamLifecycleState",
    "StreamReadiness",
    "StreamStateSnapshot",
    "TimeWindow",
    "TimeframeSpec",
    "align_to_grid",
    "build_continuity_report",
    "can_transition",
    "ceil_to_grid",
    "classify_against_existing",
    "decimal_to_canonical_text",
    "find_gaps",
    "get_timeframe",
    "iter_fetch_windows",
    "last_closed_open_time_ms",
    "parse_canonical_decimal_text",
    "parse_decimal",
    "project_stream_readiness",
    "strict_aggregate_readiness",
    "transition_stream_state",
    "validate_observed_candle",
]
