# ADR-009: Per-stream persisted state machine

**Status:** Accepted

## Context

Different streams can be bootstrapping, repairing, or ready independently. Restart recovery must be explicit.

## Decision

Persist one state machine per stream with these states:

```text
uninitialized
bootstrapping
auditing
repairing
connecting
ready
degraded
failed
```

Persisted `ready` is revalidated after restart.

## Consequences

- BTC and ETH failures are isolated.
- Recoverable bootstrap REST failures use `degraded` and may resume to `bootstrapping`.
- `failed` is reserved for states requiring manual inspection, such as corrupt storage, invalid configuration, or impossible invariants.
- Repair always returns through audit before readiness.
- Aggregate readiness is strict across required streams.

## Rejected alternatives

- One global service state.
- Trusting stale persisted readiness after restart.
