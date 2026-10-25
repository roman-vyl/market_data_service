## ADDED Requirements

### Requirement: An open-interest stream is handed off only after both tracks are proven
For a stream with `open_interest = true`, startup reconciliation SHALL reconcile open interest over `[open interest lower bound, now)` with the same gap repair after the candle window is proven continuous, and the stream SHALL enter the connecting phase only when both audits report no gaps. Until then it SHALL stay in the existing auditing state with the existing blocking reason and backoff.

#### Scenario: Open interest still has gaps at start
- **WHEN** candles of a flagged stream are continuous but its open interest audit reports gaps
- **THEN** the stream is not handed off to realtime and stays not ready

## REMOVED Requirements

### Requirement: Open interest history does not touch the live candle pipeline
**Reason**: Open interest is now a mandatory track of a flagged stream and is part of its readiness.
**Migration**: Readiness, recovery and publication rules for flagged streams are defined in runtime-health-and-readiness, realtime-ingestion-and-recovery and committed-bar-notifications.

### Requirement: Open interest work shares REST serialization and keeps its tail
**Reason**: The separate background runner and its periodic re-scan of the whole history are removed; open interest is reconciled in startup reconciliation and extended by live fetch and realtime recovery.
**Migration**: Startup open interest work runs under the existing operation gate inside startup reconciliation.
