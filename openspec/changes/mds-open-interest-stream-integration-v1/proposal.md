# MDS open interest stream integration v1

## Status
Draft for owner review (2026-10-25), rewritten to the owner's target model of 2026-10-25 15:09. No code.

## Why
Step 1 (`mds-open-interest-history-track-v1`, archived) loads open interest (OI) history for flagged 5m streams. Runtime is
still notified as soon as OHLCV is committed, with or without OI, and the history track re-scans the whole multi-year history
every 300 s to follow the tail.

For Strategy Runtime OI is a required part of market data. In normal operation the OI history is already prepared and
continuous, so MDS only has to continue it bar by bar, driven by the bars it already publishes.

## What changes
- **One publication check before send.** `CommittedBarNotificationWorker` keeps its queue, order and delivery. Before it sends
  the notification for bar T of a stream with `open_interest = true`, it runs a small publication check: audit OI continuity over
  the required range, fetch only the missing points through the existing OI repair (`RepairStreamGaps` + `ImportOpenInterestWindow`),
  audit again. Only a continuous range ending at T is published. Otherwise the notification is not sent and the reason is logged.
- **One trigger for all three paths.** Live WebSocket commits, restored REST recovery episodes and the restart path already end in
  this worker, so the check covers all of them. No other code path changes.
- **No own retry.** A failed check is retried by the next notification of the same stream (normally the next bar, 5 minutes later),
  whose range also covers the points missing before it.
- **History track without the timer.** The OI history track stays as backfill and audit: one pass to completion per stream after
  start, then it stops. The periodic re-scan of the whole history (`MDS_OI_TRACK_REFRESH_SECONDS`) is removed in this change, so
  new OI points are fetched only by the publication check.

## Unchanged
WebSocket connector, candle handler, ingestion and `COMMITTED`; OHLCV validation; candle REST recovery and its single notification
per restored episode; stream lifecycle states and READY; notification payload, queue and one-attempt HTTP delivery; Runtime webhook
contract; separate OHLCV and OI tables; every stream without the flag.

## Out of scope (next stage)
OI in the candle read API, Strategy Engine, StrategySpec, Research, backtesting, data hashes, readiness diagnostics. Premium index.
Synthetic OI values: never written by the live pipeline; unrecoverable historical holes are fixed once by a separate documented
procedure (`OI_MANUAL_FIXES.md`).
