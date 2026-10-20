# MDS restored-bar notification v1

## Why
Runtime is told about a closed bar only when the WebSocket commits it. When a bar is lost, the following happens (2026-10-18/20):

1. T1 is lost, T2 arrives over WebSocket. MDS commits T2, the supervisor sees the T1 hole, the stream becomes DEGRADED.
2. Runtime gets the webhook for T2 and calls Strategy Engine. Engine refuses (stream not ready, `MarketStreamNotReadyError`). This is correct fail-closed behaviour; no order is touched.
3. REST recovery restores T1, the stream returns to READY.
4. Nobody asks Runtime to retry T2. The trading cycle for T2 is lost.

The same applies when the WebSocket is silent and REST restores the bar: Runtime never hears about it.

## What changes
Each recovery episode (from the first signal until `restored`) carries a sticky flag `retry_needed`. It becomes true when either:
A. any REST attempt of the episode committed at least one bar (not only the last attempt);
B. a WebSocket bar was committed while the stream was recovery-pending (including the bar that revealed a sequence gap). Runtime got that bar while the stream was DEGRADED and Engine refused it.

After the episode ends `restored`, and only after the stream's durable state has been synchronised (READY again), MDS sends exactly one ordinary closed-bar notification for `restored_through_open_time_ms` if `retry_needed` is true. The flag is cleared with the episode.

- Ten restored bars T1..T10: one notification for T10, not ten.
- Episode where no REST attempt committed anything and no WebSocket bar arrived meanwhile (plain reconnect, startup reconciliation, no hole): no notification.
- `incomplete` or failed attempts do not notify by themselves; the flag survives them until `restored`.
- Same payload, same queue, same worker and same single-attempt delivery as WebSocket notifications.

## Unchanged
Recovery planning, windows, the intent worker, 60/360 s stale schedule, the stale rule, readiness, `realtime_live`, WebSocket notifications, the historical reconciler (separate wiring, never notifies), Runtime and Engine.

## Out of scope
- Bars closed while MDS was down (repaired silently by the startup reconciler).
- Runtime handling of a repeated notification for a bar it already tried: to be confirmed in strategy_runtime, no change made here.
- Durable outbox or delivery retries.
