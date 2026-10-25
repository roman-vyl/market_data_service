## 1. Readiness contract
- [ ] 1.1 Supervisor: `open_interest_frontier_ms` for flagged streams; live OI append (+1 step / discontinuity); `detect_stale` on the
      lower frontier; `record_recovery_result` and `synchronize_*` for both frontiers
- [ ] 1.2 Tests: flagged stream not `data_ready` while OI frontier is behind the expected bar after grace; unflagged unchanged

## 2. Startup
- [ ] 2.1 `HistoricalStreamReconciler`: OI reconciliation after candle `complete`, before `mark_connecting`; `admit` sets OI frontier
- [ ] 2.2 Remove `OpenInterestTrackRunner`, `oi_track_refresh_seconds`, `MDS_OI_TRACK_REFRESH_SECONDS`
- [ ] 2.3 Tests: flagged stream with OI hole is not handed off; complete one is handed off with both frontiers

## 3. Live and recovery
- [ ] 3.1 Notification worker: live OI append for flagged streams (dedicated REST client, 1 attempt, short timeout), report to
      supervisor, send only under D5
- [ ] 3.2 `RealtimeRecoveryCoordinator`: OI `RepairStreamGaps` over the same window; `RESTORED` only if both complete;
      `committed_bars` of both
- [ ] 3.3 Tests: call chains 1-6 of the design; WebSocket never waits; unflagged notifications identical

## 4. Verification
- [ ] 4.1 `make verify`, `openspec validate --strict`
- [ ] 4.2 Smoke on a copy of the production database on the owner's Mac (SOL flagged): READY only with OI, OI outage -> DEGRADED ->
      restored with one notification, Runtime webhook unchanged
