## ADDED Requirements

### Requirement: Open-interest stream readiness requires complete open interest history
A stream whose instrument has `open_interest = true` SHALL be publicly ready only when, in addition to the existing readiness rules, its open interest history track has completed an audit with no gaps. This SHALL use the existing per-stream blocking reasons of the runtime status store, with reason `open_interest_history`.

#### Scenario: History still loading
- **WHEN** a flagged stream is otherwise ready but its open interest history audit still has gaps
- **THEN** the stream is not ready and its blocking reason is `open_interest_history`

### Requirement: Late live open interest does not revoke readiness
A ready flagged stream whose latest committed bar waits for its open interest SHALL stay ready. `/readiness` SHALL expose the waiting `open_time_ms` as a diagnostic.

#### Scenario: Live open interest is late
- **WHEN** a ready flagged stream has committed bar T and is waiting for open interest at T
- **THEN** the stream stays ready and `/readiness` shows T as the pending open interest bar
