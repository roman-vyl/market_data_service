# Design

## Context (verified at bd786a4)
- `RuntimeRealtimeCoordinator.on_outcome` enqueues a notification for WebSocket `COMMITTED` outcomes and then runs `supervisor.observe_outcome` and `_sync_lifecycle()` in the same coroutine; `put_nowait` does not yield, so the stream is already DEGRADED when the worker starts the HTTP send.
- Recovery runs in `RealtimeRecoveryWorker`; `run()` calls `self._sync_lifecycle()` after each `_execute`. `RealtimeRecoveryResult` carries `restored_through_open_time_ms`; the repair counters say how many bars were committed.
- Engine `LoadLiveFeatureFrame` raises `MarketStreamNotReadyError` when bounds state is not `ready`; MDS range reads refuse a non-READY lifecycle. Runtime then records `strategy_cycle_dispatch_failed` and sends no order commands.
- The historical reconciler uses `wiring.repair()`, a separate `RepairStreamGaps` instance, and does not pass through the realtime worker.

## Decision
### D1. Owner of the event
`RuntimeRealtimeCoordinator` (runtime layer) owns "a canonical bar became live-relevant". Application and ports layers return facts only.

### D2. Trigger
The recovery worker takes an optional callback `on_restored(stream, open_time_ms)`. It is invoked in `run()` after `_execute` returned and after `self._sync_lifecycle()`, only when:
- the result is `RESTORED` and no requirement changed during the attempt (same condition under which supervisor gets `restored=True`);
- `restored_through_open_time_ms` is set;
- the recovery committed at least one bar (`committed > 0` in the repair result, summed over windows already reflected in the final result).

The coordinator's callback enqueues a `CommittedBarNotification` (if the notifier worker exists) into the existing queue.

### D3. Why after sync
If sent earlier, Engine would hit a non-READY stream again. After `_sync_lifecycle()` the durable state is READY when the supervisor says `data_ready`. If the state is still not READY, Engine fails closed as today.

### D4. One notification per recovery
Only `restored_through` is sent, never the intermediate bars: a trading decision is made for the latest closed bar on a complete history, not retroactively for each restored bar.

### D5. Not sent when nothing was restored
`committed == 0` (reconnect, startup, duplicate-only windows) means Runtime already received every bar over WebSocket; sending would repeat a handled bar.

## Risks
- A bar already handled may be sent again if a hole below it was repaired. Engine decides on the complete history, so that is the intended retry; whether Runtime tolerates a repeat for the same bar must be confirmed in strategy_runtime.
- Queue overflow drops the notification (existing best-effort contract).
- Unrelated gap repaired in the same recovery as an already traded latest bar: one repeat notification, accepted.
