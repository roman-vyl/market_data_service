## 1. Implementation
- [ ] 1.1 Expose the number of committed bars on the realtime recovery result
- [ ] 1.2 `RealtimeRecoveryWorker`: optional `on_restored` callback, called after `_sync_lifecycle()` under the D2 conditions
- [ ] 1.3 `RuntimeRealtimeCoordinator`: callback enqueues the notification (no-op without notifier worker)

## 2. Tests
- [ ] 2.1 Lost T1, T2 via WebSocket: one notification T2, after lifecycle sync
- [ ] 2.2 Silent WebSocket: one notification
- [ ] 2.3 Ten restored bars: one notification
- [ ] 2.4 Committed zero, incomplete, failed, fatal, changed-during-attempt: none
- [ ] 2.5 Historical reconciler: none
- [ ] 2.6 Notifier disabled: no error

## 3. Verification
- [ ] 3.1 `make verify` and `openspec validate --all --strict` green
