## Context

Audited main 408a1f5 uses one combined readiness predicate over subscription, recovery, candle frontier, optional OI frontier and the expected bar computed with grace. RuntimeStatusStore holds a projection. OpenInterestPublication currently mutates supervisor facts without directly refreshing that projection. Existing runtime and OI integration specs remain authoritative.

## Goals / Non-Goals

Reduce repeated lifecycle snapshot reads by selecting synchronization triggers. Preserve existing readiness ownership and maintain an explicit update path for OI mutations. SQLite, operation_gate, recovery scheduling and publication eligibility stay within their existing contracts.

## Decisions

1. In on_event, retain the admission check and supervisor.observe_event. Return before lifecycle synchronization for exactly unconfirmed CandleObserved, HeartbeatObserved and Connected. Connected currently changes only transport activity. Keep the current conservative path for every other event type, including both synchronization points surrounding confirmed ingestion.
2. Retain detect_stale plus the unconditional full lifecycle sweep in the existing periodic worker. The configured interval stays unchanged. The accepted observable timing difference is that time-driven projection updates occur at the next sweep rather than at a transport-only event. Under a blocked loop this can take longer than the nominal interval.
3. Allow the existing lifecycle synchronization method to select one StreamKey for the OI callback. Its default remains the existing full traversal. Reuse the same transition decisions, recorder and status update. Existing outcomes, recovery, admission, disconnect and periodic callers keep their full traversal.
4. In OpenInterestPublication.prepare, compare supervisor state before and after observe_open_interest using OI frontier and recovery/status fields. After recovery signals have been forwarded, synchronously refresh that stream when these fields changed, before evaluating published_through. Do this for recovery-state changes as well as frontier advancement. Failed appends and duplicate observations with unchanged facts add no synchronization. Prefetch stores data without advancing the frontier and remains outside this callback.
5. Use no durable state cache, batching, new supervisor, new locks or new await in the lifecycle operation. Runtime composition explicitly supplies the OI callback. Publication still uses the fresh supervisor predicate and the existing notification worker.
6. Verify snapshot call counts using deterministic fakes. Verify readiness using the real coordinator, supervisor, lifecycle recorder and temporary SQLite at time boundaries. Preserve existing commit-before-notification and recovery-before-notification tests.

## Risks / Trade-offs

- A transport-only event can arrive after grace but before the next periodic sweep. The stored public projection can retain its previous value until that sweep. This timing change is accepted for this step. Fresh supervisor facts and publication eligibility continue to evaluate the current clock.
- OI can change readiness without a subsequent WS event. The explicit single-stream callback covers this case and also propagates discontinuities.
- Multiple streams can become overdue together. The full periodic sweep must remain unconditional, including when detect_stale returns no signals.
- Scope can drift toward addressable recovery/outcome synchronization. Restrict the new single-stream selection to the OI callback in this change.

## Migration Plan

The proposal is ready for review. Implementation and deployment follow separately after approval. Validate with regression tests and make verify. Compare existing lifecycle diagnostics after a separately approved deployment. The workload-based estimate is approximately 94 percent fewer lifecycle snapshot calls, rather than a guaranteed rate for every configuration.
