# MDS restored-bar notification v1

## Why
Runtime is told about a closed bar only when the WebSocket commits it. When a bar is lost, the following happens (2026-10-18/20):

1. T1 is lost, T2 arrives over WebSocket. MDS commits T2, the supervisor sees the T1 hole, the stream becomes DEGRADED.
2. Runtime gets the webhook for T2 and calls Strategy Engine. Engine refuses (stream not ready, `MarketStreamNotReadyError`). This is correct fail-closed behaviour; no order is touched.
3. REST recovery restores T1, the stream returns to READY.
4. Nobody asks Runtime to retry T2. The trading cycle for T2 is lost.

The same applies when the WebSocket is silent and REST restores the bar: Runtime never hears about it.

## What changes
After a realtime recovery ends `restored`, and only after the stream's durable state has been synchronised (READY again), MDS sends exactly one ordinary closed-bar notification for `restored_through_open_time_ms`, if that recovery committed at least one bar.

- Ten restored bars T1..T10: one notification for T10, not ten.
- Recovery that committed nothing (reconnect or startup reconciliation with no hole): no notification.
- `incomplete` or failed recovery: no notification.
- Same payload, same queue, same worker and same single-attempt delivery as WebSocket notifications.

## Unchanged
Recovery planning, windows, the intent worker, 60/360 s stale schedule, the stale rule, readiness, `realtime_live`, WebSocket notifications, the historical reconciler (separate wiring, never notifies), Runtime and Engine.

## Out of scope
- Bars closed while MDS was down (repaired silently by the startup reconciler).
- Runtime handling of a repeated notification for a bar it already tried: to be confirmed in strategy_runtime, no change made here.
- Durable outbox or delivery retries.
