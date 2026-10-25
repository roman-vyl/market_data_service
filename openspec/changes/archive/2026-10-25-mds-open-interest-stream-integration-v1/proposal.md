# MDS open interest stream integration v1

## Status
Draft for owner review (2026-10-25), rewritten after the owner's review of 15:38. No code.

## Why
For a stream whose instrument has `open_interest = true`, OHLCV and open interest (OI) are one mandatory data set for Strategy
Runtime. Today OI lives in a side track that never affects READY, follows the tail with a 300 s re-scan of the whole history, and a
bar is published to Runtime without its OI. A stream can be READY while its OI is missing or broken.

## Main invariant
For a flagged stream, READY (and the right to publish a bar to Runtime) means at the same time:
- OHLCV continuity is proven (audited) from its lower bound;
- OI continuity is proven (audited) from its lower bound;
- both are current up to the last closed bar, under the same expected-bar rule and grace;
- no recovery of either is pending or unfinished.
One definition of readiness, used both by MDS state and by publication. Unflagged streams keep today's definition unchanged.

## What changes
- **Two tracks, one stream state.** The existing per-stream owners carry both tracks: startup reconciliation, the realtime
  supervisor (continuity frontier and expected-bar rule), realtime recovery (`RepairStreamGaps` for each track over the same
  window) and the lifecycle (`READY` / `DEGRADED` / ...). No new lifecycle states, tables or watermarks.
- **Proven frontier, not latest point.** Each track has a continuity frontier owned by the supervisor: set only from an audited
  range (startup or recovery) or advanced by exactly one step by a live append. A stored point beyond a hole never moves it.
- **Different sources and triggers, same requirements.** Candles: WebSocket live, REST in recovery. OI: one REST window for bar T
  fetched when candle T is committed (off the event loop), REST in recovery. Integrity, continuity, recovery and readiness rules are
  the same for both.
- **Startup hand-off needs both.** A flagged stream is handed to realtime only after both reconciliations are complete. The
  separate background OI runner and its timer are removed; OI reconciliation runs inside startup reconciliation.
- **Publication follows readiness.** A flagged bar is sent to Runtime only when the stream is data-ready through that bar.
  Otherwise the existing recovery episode publishes it once when it ends restored.

## Unchanged
Notification payload, queue, worker and one-attempt delivery; Runtime webhook contract; WebSocket connector and candle ingestion;
`COMMITTED` = OHLCV valid and stored; OHLCV validation; separate OHLCV and OI tables; the one-notification-per-restored-episode rule;
every unflagged stream.

## Out of scope (next stage)
OI in the candle read API, Strategy Engine, StrategySpec, Research, backtesting, data hashes. Synthetic OI values are never written by
the runtime; unrecoverable historical holes are fixed once by a separate documented procedure (`OI_MANUAL_FIXES.md`).
