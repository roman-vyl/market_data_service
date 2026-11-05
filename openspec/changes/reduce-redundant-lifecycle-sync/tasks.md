## 1. Dispatch and OI projection

- [ ] 1.1 Retain supervisor observation but skip lifecycle sync for unconfirmed CandleObserved, HeartbeatObserved and Connected only.
- [ ] 1.2 Add single-stream selection to the existing sync method for the explicit OI callback and wire it into publication preparation.
- [ ] 1.3 Invoke the callback for OI frontier or recovery/status changes before publication eligibility, preserving other full-sweep callers.

## 2. Regression verification

- [ ] 2.1 Verify no lifecycle reads for intermediate candles/heartbeat/Connected and preserve transport facts, confirmed pre/post synchronization, rejection and fatal handling.
- [ ] 2.2 Verify before/at/after close plus grace for candle and OI, silent streams, several simultaneous changes and periodic refresh without new stale signals.
- [ ] 2.3 Verify OI readiness advancement and discontinuity without a later WS event, callback-before-publication ordering, unchanged duplicates/failed appends, and single-stream isolation.
- [ ] 2.4 Preserve reconnect, recovery notification ordering, restart reconciliation, startup and late-admission coverage. Run make verify.
- [ ] 2.5 Compare deterministic snapshot counts with the former full-event traversal and review the scope before requesting deployment approval.
