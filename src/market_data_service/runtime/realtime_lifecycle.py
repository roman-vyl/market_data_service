"""Project one stream's realtime facts into its existing durable lifecycle."""

from market_data_service.application.realtime.supervisor_types import RealtimeStreamFacts
from market_data_service.domain.stream_state import StreamLifecycleState, StreamStateSnapshot
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.status import RuntimeStatusStore


def synchronize_stream_lifecycle(
    facts: RealtimeStreamFacts,
    lifecycle: RuntimeLifecycleRecorder,
    status: RuntimeStatusStore,
    *,
    admitted: bool,
    durable: StreamStateSnapshot,
) -> None:
    if not admitted:
        status.update_stream(durable, facts)
        return
    if facts.fatal_error_code and durable.state is not StreamLifecycleState.FAILED:
        durable, _ = lifecycle.transition_if_current(
            durable, StreamLifecycleState.FAILED, reason=facts.fatal_error_code
        )
    elif facts.data_ready:
        if durable.state is StreamLifecycleState.DEGRADED:
            durable, applied = lifecycle.transition_if_current(
                durable, StreamLifecycleState.CONNECTING
            )
            if not applied:
                status.update_stream(durable, facts)
                return
        if durable.state is StreamLifecycleState.CONNECTING:
            durable, _ = lifecycle.transition_if_current(durable, StreamLifecycleState.READY)
    elif (
        facts.recovery_pending
        or not facts.current
        or facts.status.value in {"disconnected", "stale", "recovery_required"}
    ) and durable.state in {
        StreamLifecycleState.CONNECTING,
        StreamLifecycleState.READY,
    }:
        reason = facts.status.value if facts.current else "stale"
        durable, _ = lifecycle.transition_if_current(
            durable, StreamLifecycleState.DEGRADED, reason=reason
        )
    status.update_stream(durable, facts)
