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
  only while every writer keeps the stored OI continuous up to it (D3, D5).
- The Bybit REST client retries 4 times with a 10 s timeout (worst case about 1 minute per call).
- The OI history runner reschedules a full pass `[oi_lower_bound, now]` every `oi_track_refresh_seconds` (300 s) under the shared
  `operation_gate`.

## Decisions

### D1. Publication check in the notification worker
`CommittedBarNotificationWorker` gets one optional collaborator, the publication check, called in `asyncio.to_thread` right
before `send`. It answers one question: may bar T of this stream be published now? The worker stays the only owner of the queue,
the order and the delivery. The check has no queue, no task, no timer, no lifecycle and no stored state of its own. For a stream
without the flag it answers yes without I/O, so those streams behave exactly as today. A no answer drops that notification with a
warning log (stream, T, reason); WebSocket, ingestion and `COMMITTED` are not affected.

### D2. Continue from the last stored point
For bar T with step s let `L` be the latest stored OI point of the stream (track progress). The prepared history is continuous up
to `L` (it is loaded and audited before the flag is used), and D3 keeps it that way, so `L` is the continuation point:
- `L >= T`: OI(T) is stored, yes without REST (stored OI is never fetched again);
- `L < T`: the missing range is `[L + s, T + s)`;
- `L` unknown (history not prepared): no, `oi_history_not_prepared`, no fetch.
No lookback window, no extra watermark, no new state.

### D3. Fetch only a continuous continuation
The check fetches `[L + s, T + s)` in windows of at most 200 points, oldest first, at most `W` windows per check
(`MDS_OI_PUBLISH_WINDOWS_PER_CHECK`, default 2), through the existing `ImportOpenInterestWindow`, with one small addition: the window's points are cut at the first
missing `open_time_ms` before they are stored. It stores only the continuous
run of points that starts exactly at `L + s`; a point after the first missing one is not stored, and the check stops there. So `L`
only ever moves over continuous data, and a partial Bybit answer can never leave a hole behind `L`.
Result: stored through T -> yes; stopped earlier (missing point, more than `W` windows, REST failure) -> no with the reason.
Values come only from Bybit points with exactly the bar's `open_time_ms`; nothing is interpolated or copied from a neighbour.

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
