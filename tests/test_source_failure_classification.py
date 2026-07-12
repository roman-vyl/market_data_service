from market_data_service.adapters.bybit.errors import (
    BybitApiError,
    BybitHttpError,
    BybitPayloadError,
    BybitTransientApiError,
)
from market_data_service.application.source_failure import (
    SourceFailureDisposition,
    classify_source_failure,
)
from market_data_service.domain import StreamLifecycleState


def test_transport_and_approved_transient_bybit_failures_are_recoverable() -> None:
    for error in (BybitHttpError("timeout"), BybitTransientApiError("rate limit")):
        decision = classify_source_failure(error)
        assert decision.disposition is SourceFailureDisposition.RECOVERABLE
        assert decision.target_state is StreamLifecycleState.DEGRADED


def test_payload_api_config_and_storage_failures_are_fatal() -> None:
    for error in (
        BybitPayloadError("malformed"),
        BybitApiError("invalid request"),
        ValueError("invalid config"),
        RuntimeError("storage invariant"),
    ):
        decision = classify_source_failure(error)
        assert decision.disposition is SourceFailureDisposition.FATAL
        assert decision.target_state is StreamLifecycleState.FAILED
