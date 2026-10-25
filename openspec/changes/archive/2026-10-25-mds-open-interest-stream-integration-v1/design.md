# Design

## Context (verified at main 85cdc4e)
- Candle readiness is built from existing owners: startup reconciliation (`runtime/reconciliation.py`
  `HistoricalStreamReconciler`: lower bound, `RepairStreamGaps` over `[lower bound, now)`, `AUDITING` -> `CONNECTING`), hand-off
  (`runtime/historical_worker.py` -> `runtime/realtime.py` `admit`), the realtime supervisor (`application/realtime/supervisor.py`:
  continuity frontier `last_successful_open_time_ms`, gap -> `SEQUENCE_DISCONTINUITY`, expected-bar rule with grace -> `STALE`),
  realtime recovery (`runtime/realtime_recovery_worker.py`, `application/realtime/recovery.py`: `RepairStreamGaps` over the window,
  `RESTORED` only when the post-audit is continuous and the target did not advance) and `_sync_lifecycle` (`data_ready` -> `READY`,
  pending recovery -> `DEGRADED`).
- The candle frontier is not "the latest stored point": it is set from an audited range (startup, recovery) or advanced by a live
  commit, and a jump of more than one step raises a discontinuity.
- OI already has the same building blocks: `ImportOpenInterestWindow`, OI lower bound resolution, `open_interest_open_times` audit
  and `RepairStreamGaps` wired for OI (`RuntimeWiring.open_interest_repair()`), plus `OpenInterestTrackReconciler`. Today they run in
  a separate background runner (`runtime/open_interest_runner.py`) that never touches readiness and re-scans the whole history every
  300 s.
- Every committed-bar notification goes through `CommittedBarNotificationWorker` (one consumer, off the event loop for I/O).

## D1. One readiness contract
For a flagged stream, `data_ready` (supervisor fact) and therefore durable `READY` require: both tracks proven continuous from their
lower bounds, both frontiers at or after the expected bar (same `align(now - grace) - step` rule, same grace), and no pending or
unfinished recovery of either track. Implemented by extending the existing owners only:
- supervisor facts get a second frontier, `open_interest_frontier_ms`, for flagged streams;
- `detect_stale` uses the lower of the two frontiers;
- `recovery_pending` / `recovery_restored` keep covering the whole stream, so `_sync_lifecycle`, `RuntimeStatusStore` and
  `/readiness` need no change.
Unflagged streams: no OI frontier, today's behaviour.

## D2. Proven continuity, not the latest point
Each frontier moves only in two ways, identical for both tracks:
1. set to the end of an audited continuous range (startup reconciliation, restored recovery);
2. advanced by exactly one step by a live append (candle from WebSocket, OI from the live fetch, D4).
A live append more than one step ahead raises `SEQUENCE_DISCONTINUITY` from frontier + step, as for candles today. A point stored
beyond a hole (partial REST answer) never moves a frontier. `track_progress.latest_committed_open_time_ms` is not used for readiness.

## D3. Startup: both tracks before hand-off
`HistoricalStreamReconciler.execute` for a flagged stream runs, after the candle repair is `complete`, the OI reconciliation over
`[OI lower bound, now)` with the same `RepairStreamGaps` (OI wiring) and budget rules, and only then `mark_connecting`. Until both
are complete the stream stays `AUDITING`, the existing historical worker keeps it blocked (`historical_reconciliation`) and backs
off on failure. `admit` sets the candle frontier as today and the OI frontier to the end of the audited OI range.
`OpenInterestTrackRunner`, its timer and `MDS_OI_TRACK_REFRESH_SECONDS` are removed; `OpenInterestTrackReconciler` logic is reused
inside startup reconciliation. A flagged stream whose OI history is not loaded is therefore not READY until it is.

## D4. Live: OI for bar T is fetched when candle T is committed
WebSocket stays the only live candle source and never waits for REST. For a flagged stream, the existing notification worker,
which already consumes every `COMMITTED` outcome off the event loop, first performs the live OI append for T: one REST window
`[T, T + s)` through `ImportOpenInterestWindow` (dedicated Bybit client: 1 attempt, short timeout, no operation gate), then reports
the result to the supervisor (frontier + 1 step, or a discontinuity, or nothing on failure). It then sends the notification only if
D5 holds. A failed fetch is not retried there: if OI(T) is not stored by the end of the grace, `detect_stale` sees the OI
frontier behind, the stream goes to recovery and `DEGRADED`, exactly as a missing WebSocket candle does.

## D5. Publication follows readiness
A flagged bar T is sent only when the stream is `data_ready` and both frontiers are at or after T. Otherwise the notification is
not sent; the live commit has already marked the active recovery episode (`mark_live_commit`), so the episode sends one
notification for its last restored bar when it ends restored (existing rule). Unflagged streams are notified as today.

## D6. Recovery repairs both tracks over one window
`RealtimeRecoveryCoordinator` for a flagged stream: the window starts at the earliest of candle latest + step, the signal hint
(which carries the lower frontier + step) and ends at the last closed bar, as today. It runs the candle `RepairStreamGaps`, then the
OI `RepairStreamGaps` over the same window. `RESTORED` only if both are `complete` with continuous post-audits and the target did
not advance; `committed_bars` counts both tracks, so OI repaired by an episode also triggers its single notification.
`record_recovery_result` sets both frontiers to `restored_through_open_time_ms`. Failure of either track = the existing
`incomplete` / failed handling and backoff of the one intent of the stream.

## D7. Concurrency and budgets
- The live OI fetch runs in a worker thread; its SQLite write is the existing short import transaction, after the REST call.
- Recovery and startup OI work run under the existing `operation_gate` with the existing per-pass budgets.
- Normal live OI fetch: one point, well under a second. Bybit down: about one timeout per flagged bar on the shared queue.

## Call chains
| # | Case | Chain through existing owners | Result |
| --- | --- | --- | --- |
| 1 | Normal live bar | connector -> handler -> ingest `COMMITTED` -> `on_outcome` (frontier +1) -> worker: OI fetch T (OI frontier +1) -> D5 holds -> send | T sent with OI, stream READY |
| 2 | OI(T) not available in time | as 1, fetch fails -> no send -> grace ends -> `detect_stale` (OI frontier behind) -> `STALE` -> recovery: candles nothing, OI repairs T -> `RESTORED` -> one send | READY -> DEGRADED -> READY, T sent once |
| 3 | WebSocket lost bar T1, T2 arrives | `observe_outcome` gap -> `SEQUENCE_DISCONTINUITY` -> recovery from T1: both tracks -> `RESTORED` -> one send for the last bar; live T2 not sent (D5) | no bar published while either track has a hole |
| 4 | WebSocket silent | `detect_stale` -> recovery of both tracks -> one send | as today, plus OI |
| 5 | Restart | startup reconciliation: candles then OI -> `admit` (both frontiers) -> `STARTUP_RECONCILIATION` recovery of both -> one send if anything was committed | READY only after both are proven |
| 6 | Crash between candle T and OI(T) | as 5: the window from the lower frontier includes T, OI(T) is repaired, one send | |
| 7 | OI history has a hole at start | startup OI repair; unrecoverable -> stays `AUDITING`/backoff, not READY | manual fix procedure, then restart |
| 8 | Duplicate, corrected, rejected candle | unchanged (`REJECTED` -> recovery of both tracks) | |

## Failure modes
- Bybit OI REST down while WebSocket works: candles keep being committed (`COMMITTED` unchanged), flagged streams go `DEGRADED`
  after the grace, no bar is published, recovery backs off as for candles; when REST returns, one recovery restores both and sends once.
- Queue full or POST failure: unchanged best-effort delivery; data state is not affected.

## Rollout
SOL flagged (history complete). BTC and ETH stay unflagged until their OI history is loaded; flagging them earlier keeps them not
READY by design.
