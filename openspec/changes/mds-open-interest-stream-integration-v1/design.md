# Design

## Context (verified at main 85cdc4e)
- Every committed-bar notification enters the single `CommittedBarNotificationWorker` (`runtime/committed_bar_notification.py`):
  a bounded in-memory FIFO with one consumer task; `send` already runs in `asyncio.to_thread`, so the WebSocket loop never waits
  for it. There are exactly two enqueue sites, both in `runtime/realtime.py`:
  - `on_outcome`: a live `COMMITTED` outcome;
  - `_notify_restored`: one notification for `restored_through_open_time_ms` after a restored recovery episode that committed
    bars or was marked by a live commit.
  Restart reaches the same worker: `admit` -> `STARTUP_RECONCILIATION` recovery -> `_notify_restored` when it committed bars.
- Notifications are non-durable by spec. After a crash between commit and send, with nothing to recover at start, the next
  notification is for T+1. The owner accepts this (2026-10-25).
- OI is written only by `ImportOpenInterestWindow` (one REST window of at most 200 points, fetched outside the transaction, then one
  short transaction; same value = `duplicate`, different value = `corrected`). `RepairStreamGaps`, wired as
  `RuntimeWiring.open_interest_repair()`, already audits OI continuity (`open_interest_open_times`), fetches only gap windows
  (`iter_fetch_windows`, 200 points), stops at the first REST failure and audits again (`complete` / `incomplete` / `failed`).
- `track_progress.latest_committed_open_time_ms` of the OI track is the maximum stored point. It is a valid continuation point
  the continuation point exactly as `stream_state.latest_committed_open_time_ms` is for candle recovery.
- The Bybit REST client retries 4 times with a 10 s timeout (worst case about 1 minute per call).
- The OI history runner reschedules a full pass `[oi_lower_bound, now]` every `oi_track_refresh_seconds` (300 s) under the shared
  `operation_gate`.

## Call chains: where each case enters the existing pipeline
Names are files under `src/market_data_service/`. `T` is the bar in the notification, `L` the latest stored OI point, `s` = 5m.
Every chain that publishes ends in the same place: `runtime/committed_bar_notification.py`
`CommittedBarNotificationWorker.run` -> publication check -> `_send`. Chains that do not reach the worker publish nothing today
and nothing changes for them.

| # | Case | Existing chain up to the worker | T | OI the check needs |
| --- | --- | --- | --- | --- |
| 1 | Normal live bar | `application/realtime/connector.py` `_run_session` -> `runtime/admission.py` `AdmissionGatedCandleHandler.handle` -> `application/realtime/handler.py` `handle` -> `application/ingest.py` `IngestObservedCandle` = `COMMITTED` -> `runtime/realtime.py` `on_outcome` -> `enqueue(bar_notification(T))` | the live bar | `L = T - s`: one point, T |
| 2 | Live bar after a held one (OI of T-1 failed) | same as 1 | the live bar | `L = T - 2s`: T-1 and T in one request |
| 3 | Lost WS bar, next bar arrives | T2 as in 1 (sent at once, as today). `supervisor.py` `observe_outcome` -> `SEQUENCE_DISCONTINUITY` -> `runtime/realtime_recovery_worker.py` `enqueue` / `_execute` -> `application/realtime/recovery.py` `execute` (window from T1) -> `RESTORED` -> `_notify_restored(restored_through)` | first T2, then the last bar of the recovery window | for T2: T1..T2 (OI exists on Bybit even while candle T1 is missing); for the restored notification: from `L + s`, usually nothing left |
| 4 | Silent WS (stale) | `runtime/realtime.py` `_stale_worker` -> `supervisor.detect_stale` -> `STALE` -> recovery as in 3 -> `_notify_restored` | last restored bar | `L + s` .. T = exactly the bars REST recovered |
| 5 | Disconnect / resubscribe, rejected candle | `supervisor.observe_event` / `observe_outcome` -> `DISCONNECT` / `REJECTED_OBSERVATION` -> recovery as in 3 | last restored bar | as in 4 |
| 6 | Restart, bars missed while down | `runtime/service.py` `run` -> `_startup` -> `runtime/historical_worker.py` -> `runtime/realtime.py` `admit` -> `STARTUP_RECONCILIATION` -> recovery window `[latest candle + s, now]` -> `_notify_restored` | last restored bar | `L + s` .. T: the downtime plus any bar whose OI was not stored before the stop |
| 7 | Restart, nothing missed (crash between commit and send) | `admit` -> recovery commits nothing -> no notification (today too) | none until the next live bar | next live bar as in 1/2: `L + s` .. T+1 includes T |
| 8 | Recovery `incomplete` or failed, fatal | `RealtimeRecoveryWorker._execute` retries or ends | none | none |
| 9 | Duplicate, corrected, unconfirmed, admission-blocked candle | `on_outcome` does not enqueue | none | none |
| 10 | Queue full | `enqueue` drops with an error log | none | the next notification continues from `L` |
| 11 | Runtime POST fails after the check | `_send` logs | already stored | the next notification finds `L >= T` |
| 12 | OI history backfill pass on start | `runtime/open_interest_runner.py` -> `OpenInterestTrackReconciler` -> OI `RepairStreamGaps` | never notifies | it only fills history and may move `L` forward |

So the check never needs to know which chain brought T: in every case the bars still missing OI are exactly `(L, T]`, the same way
candle recovery continues from its latest committed candle (D2).

## Decisions

### D1. Publication check in the notification worker
`CommittedBarNotificationWorker` gets one optional collaborator, the publication check, called in `asyncio.to_thread` right
before `send`. It answers one question: may bar T of this stream be published now? The worker stays the only owner of the queue,
the order and the delivery. The check has no queue, no task, no timer, no lifecycle and no stored state of its own. For a stream
without the flag it answers yes without I/O, so those streams behave exactly as today. A no answer drops that notification with a
warning log (stream, T, reason); WebSocket, ingestion and `COMMITTED` are not affected.

### D2. Same REST repair as candles
OI and candles already share one REST repair mechanism: `application/repair_gaps.py` `RepairStreamGaps` (audit -> import only the
gap windows -> audit again), with the candle importer for candles and `ImportOpenInterestWindow` + `open_interest_open_times`
for OI (`RuntimeWiring.open_interest_repair()`). The check calls exactly that, with the window chosen the same way candle
recovery chooses it (`application/realtime/recovery_plan.py`: from the latest committed point plus one step to the bar):

`RepairStreamGaps.execute(stream, start = L + s, end = T + s, max_windows = W)`, `L` = latest stored OI point.

- `L >= T`: nothing to repair, yes without REST (stored OI is never fetched again);
- result `complete` -> yes; `incomplete` (partial Bybit answer, more gaps than `W` windows) -> no, `oi_incomplete`;
  `failed` -> no, `oi_source_failed` -- the same three outcomes candle recovery acts on;
- `L` unknown (history not prepared) -> no, `oi_history_not_prepared`, no fetch.
Windows of at most 200 points (`iter_fetch_windows`), at most `W` per check (`MDS_OI_PUBLISH_WINDOWS_PER_CHECK`, default 2).
Validation, `duplicate` / `corrected` and quarantine are the importer's own. No lookback window, no extra watermark, no new state,
no OI-specific continuity logic.

### D4. REST budget on the shared queue
The check uses its own instance of the existing Bybit REST client with 1 attempt and a short timeout
(`MDS_OI_PUBLISH_TIMEOUT_SECONDS`, default 2 s). It never takes `operation_gate`, so a candle history pass or a candle recovery
never delays it. Normal case: one request of one point, well under a second. Bybit down: the first failed request ends the check,
so at most about 2 s per flagged notification (about 6 s per 5-minute boundary for 3 flagged coins). A long backlog after an
outage is filled `W` windows per notification, so the queue is never held for more than `W` timeouts by one item.

### D5. History track: one pass, no timer
`OpenInterestTrackRunner` keeps the backfill and audit pass: after start each flagged stream is reconciled over
`[oi_lower_bound, now]` until the pass completes, with the existing budget, backoff and gate yielding. After a completed pass the
stream is not rescheduled. `oi_track_refresh_seconds` / `MDS_OI_TRACK_REFRESH_SECONDS` are removed. Regular new points therefore
come from one mechanism only, the publication check. Backfill of a newly flagged coin and a full audit happen on the next start.

### D6. Concurrency
- WebSocket: unchanged path; the check runs in the worker task and a worker thread. Its SQLite writes are the existing short
  import transactions after the REST fetch, the same as the history track writes today.
- The startup history pass and the check may import the same points; the importer classifies them `duplicate`.
- Order: the worker remains FIFO; a held notification is simply not sent.

## Failure modes
- Bybit OI late or down: OHLCV stays committed, no notification for flagged streams, warning per held bar. The next bar fetches
  the missing points with its own and is published.
- Outage longer than `W` windows: notifications of that stream stay held while each new notification fills `W` more windows.
- Crash after OI import, before send: the next notification continues from the stored `L`.
- History never prepared for a flagged stream: its notifications stay held until the startup pass completes. The flag is turned on
  only after the history is prepared (BTC and ETH stay off for now).

## Rollout
Deploy with SOL flagged (history complete). BTC and ETH are flagged after their history is loaded and checked.
