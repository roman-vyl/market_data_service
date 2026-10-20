# Design

## Context (verified at bd786a4)
- `RuntimeRealtimeCoordinator.on_outcome` enqueues a notification for WebSocket `COMMITTED` outcomes and then runs `supervisor.observe_outcome` and `_sync_lifecycle()` in the same coroutine; `put_nowait` does not yield, so the stream is already DEGRADED when the worker starts the HTTP send.
- Recovery runs in `RealtimeRecoveryWorker`; `run()` calls `self._sync_lifecycle()` after each `_execute`. `RealtimeRecoveryResult` carries `restored_through_open_time_ms`; the repair counters say how many bars were committed.
- Engine `LoadLiveFeatureFrame` raises `MarketStreamNotReadyError` when bounds state is not `ready`; MDS range reads refuse a non-READY lifecycle. Runtime then records `strategy_cycle_dispatch_failed` and sends no order commands.
- The historical reconciler uses `wiring.repair()`, a separate `RepairStreamGaps` instance, and does not pass through the realtime worker.

## Decision
### D1. Owner of the event
`RuntimeRealtimeCoordinator` (runtime layer) owns "a canonical bar became live-relevant". Application and ports layers return facts only.

### D2. Episode flag `retry_needed`
The recovery intent (one per stream, already the episode owner) gets a sticky boolean `retry_needed`, false at creation. It is set:
- A: after any attempt of the episode whose repair result committed at least one bar (every attempt, including `incomplete` ones such as `recovery_target_advanced`);
- B: by the coordinator when a WebSocket outcome is `COMMITTED` and, after `supervisor.observe_outcome` and enqueue of its signals, the stream is recovery-pending. This covers a new bar closing during recovery and the bar that revealed a sequence gap.

It is never cleared by `incomplete` or failure. It is cleared only when the episode ends: `restored` with unchanged revision (after the notification decision) or fatal failure.

Example A: attempt 1 commits T1, target advanced, `incomplete`; attempt 2 finds continuity, committed 0, `restored`. Flag set by attempt 1, so one notification.
Example B: reconnect, recovery pending, T1 closes over WebSocket and is committed, Runtime sends it and Engine refuses (DEGRADED); REST audit finds no hole, committed 0, `restored`. Flag set by B, so one notification for T1.

Not designed: a WebSocket replaying an old missed bar. Bybit v5 kline WebSocket is a live feed with no replay; missed history is only restored over REST.

### D3. Trigger
The recovery worker takes an optional callback `on_restored(stream, open_time_ms)`, called in `run()` after `_execute` and after `self._sync_lifecycle()` when the result is `RESTORED`, no requirement changed during the attempt, `restored_through_open_time_ms` is set, and the episode flag is true. The coordinator's callback enqueues a `CommittedBarNotification` (no-op without a notifier worker).

### D4. Why after sync
If sent earlier, Engine would hit a non-READY stream again. After `_sync_lifecycle()` the durable state is READY when the supervisor says `data_ready`. If it is still not READY, Engine fails closed as today.

### D5. Latest-only semantics
Only `restored_through` is sent, never intermediate bars. The notification means "history through this bar is complete; decide on the latest closed bar". Complete history is covered; intermediate trading cycles (the refused WebSocket bar, the restored bars) are not replayed and no decision is made retroactively for them.

## Risks
- A bar already handled may be sent again if a hole below it was repaired. Engine decides on the complete history, so that is the intended retry. A repeated notification for the same bar is idempotent in Runtime (Runtime contract and production E2E).
- Queue overflow drops the notification (existing best-effort contract).
- Unrelated gap repaired in the same recovery as an already traded latest bar: one repeat notification, accepted.
