## 1. Implementation
- [ ] 1.1 Expose the number of committed bars on the realtime recovery result
- [ ] 1.2 `RecoveryIntent.retry_needed`: set by any attempt with committed > 0 and by a WebSocket COMMITTED outcome while recovery-pending; cleared only at episode end
- [ ] 1.3 `RealtimeRecoveryWorker`: optional `on_restored` callback after `_sync_lifecycle()` (D3); method for the coordinator to mark a live commit
- [ ] 1.4 `RuntimeRealtimeCoordinator`: mark live commit in `on_outcome` (one synchronous step after enqueueing this outcome's signals, D2); callback enqueues the notification (no-op without notifier worker)

## 2. Tests
- [ ] 2.1 Lost T1, T2 via WebSocket: one notification T2, after lifecycle sync
- [ ] 2.2 Silent WebSocket: one notification
- [ ] 2.3 Ten restored bars: one notification
- [ ] 2.4 Commit in attempt 1, committed 0 in attempt 2: one notification
- [ ] 2.4a WebSocket bar committed while recovery-pending, REST committed 0: one notification; sequence-gap bar likewise
- [ ] 2.4b No commit anywhere: none; incomplete, non-fatal failed, changed-during-attempt: no notification yet, flag kept; fatal: no notification, episode ends, flag discarded
- [ ] 2.4c WebSocket commit marks only the episode active at that moment or created by that same outcome; no active episode and no signal: nothing marked; finished or superseded episode is never marked; sequence-gap bar creates and marks its own episode
- [ ] 2.5 Historical reconciler: none
- [ ] 2.6 Notifier disabled: no error

## 3. Verification
- [ ] 3.1 `make verify` and `openspec validate --all --strict` green
