"""Project one stream's realtime facts into its existing durable lifecycle."""

from market_data_service.application.realtime.supervisor_types import RealtimeStreamFacts
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.status import RuntimeStatusStore


def synchronize_stream_lifecycle(
    facts: RealtimeStreamFacts,
    lifecycle: RuntimeLifecycleRecorder,
    status: RuntimeStatusStore,
    *,
    admitted: bool,
) -> None:
    durable = lifecycle.snapshot(facts.stream)
    if not admitted:
        status.update_stream(durable, facts)
        return
    if facts.fatal_error_code and durable.state is not StreamLifecycleState.FAILED:
        durable = lifecycle.mark_failed(facts.stream, reason=facts.fatal_error_code)
    elif facts.data_ready:
        if durable.state is StreamLifecycleState.DEGRADED:
            durable = lifecycle.mark_connecting(facts.stream)
        if durable.state is StreamLifecycleState.CONNECTING:
            durable = lifecycle.mark_ready(facts.stream)
    elif (
        facts.recovery_pending
        or not facts.current
        or facts.status.value in {"disconnected", "stale", "recovery_required"}
    ) and durable.state in {
        StreamLifecycleState.CONNECTING,
        StreamLifecycleState.READY,
    }:
        reason = facts.status.value if facts.current else "stale"
        durable = lifecycle.mark_degraded(facts.stream, reason=reason)
    status.update_stream(durable, facts)
