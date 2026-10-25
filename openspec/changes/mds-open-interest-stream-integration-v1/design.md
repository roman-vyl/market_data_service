# Design

## Context (verified at main 85cdc4e)
- A bar is published to Runtime from exactly two places, both in `runtime/realtime.py` (`RuntimeRealtimeCoordinator`):
  `on_outcome` enqueues `bar_notification(stream, T)` for a live `COMMITTED` outcome, and `_notify_restored` enqueues one
  notification for `restored_through_open_time_ms` after a restored recovery episode. Both go to the single
  `CommittedBarNotificationWorker` (bounded in-memory queue, one send in flight, one HTTP attempt). The historical reconciler never
  notifies.
- Notifications are non-durable by spec (no outbox, no retry loop). A crash between commit and send loses that notification today.
- `/v1/candles` (`GetCandleRange`) serves a READY stream inside `[earliest_available, latest_committed + step)` and requires a
  complete grid. `/v1/historical-candles` bypasses readiness but requires an exact `market_data_hash` match.
- `market_data_hash` = SHA-256 of ticker, timeframe, range and the ordered OHLCV documents (`consumer_read/provenance.py`). Research
  artifacts and Engine runs store these hashes and replay them through `/v1/historical-candles`.
- Per-stream readiness = durable `READY` and realtime `data_ready`; the runtime status store also holds per-stream blocking reasons
  that keep a stream not ready. The OI history runner currently reports into a log-only sink (`LogOnlyBlockingReasons`).
- The shared `operation_gate` (asyncio lock) serializes candle REST recovery, candle history and OI history passes. An OI history
  pass with `MDS_OI_TRACK_WINDOWS_PER_PASS=20` holds it for up to ~60 s on the owner's Mac. Live candle ingestion does not take it.
- Engine reads MDS with pydantic models that use `extra="forbid"`: any new field in a candle document breaks the Engine client.

## Decisions

### D1. `COMMITTED` and `READY_TO_PUBLISH`
`COMMITTED` is unchanged. `READY_TO_PUBLISH(stream, T)` is a derived condition, evaluated from storage, not a stored state:
`COMMITTED(T)` and (`open_interest = false` or OI row `(stream, T)` exists). Ingestion, repair, realtime recovery and the stream
lifecycle never look at it. Only the two notification sources and the read API use it.

### D2. Live completion without blocking the WebSocket
`on_outcome` for a flagged stream hands `(stream, T)` to `OpenInterestCompleter` instead of enqueuing the notification. The completer:
1. returns immediately to the caller (own bounded queue, one task, like the notification worker);
2. if OI(T) is already stored, enqueues the notification at once;
3. else fetches the OI window `[T, T+step)` in `asyncio.to_thread`, imports it with `ImportOpenInterestWindow`, and on success
   enqueues the notification into the existing worker;
4. if Bybit has no point yet, retries the same bar with a capped schedule (1, 2, 4, 8, 15, 30, 30, ... s), logging each retry. It never
   publishes without OI(T) and never substitutes another value.

Live priority: the completer does **not** take `operation_gate`. It writes like live candle ingestion does (its own short
transaction), so a long OI or candle history pass never delays it. The OI history worker keeps yielding to the gate as today.

### D3. Readiness of a flagged stream (owner choice)
- **A (recommended):** a flagged stream is ready only when its OI history track is complete (last audit gaps = 0 over
  `[oi_lower_bound, latest OI]`). The OI runner reports `open_interest_history` as a blocking reason through the existing status
  store instead of the log-only sink. Turning the flag on for a coin with no OI history makes that stream not ready until the
  history is loaded, so it is done only with Runtime off or on a coin Runtime does not trade.
- **B:** readiness unchanged; only publication and reads are gated. Simpler, but `/readiness` can say ready while `/v1/candles`
  rejects most ranges.

A live bar waiting for OI(T) does not change readiness in either option: publication is delayed, the stream stays ready, and
`/readiness` shows `open_interest_pending_open_time_ms` for diagnostics.

### D4. Recovery and restart
- Restored episode: `_notify_restored(stream, T_last)` for a flagged stream goes to the completer with the range
  `(oi_watermark, T_last]`; the completer fills OI for every committed bar in it (windows of 200), then enqueues one notification
  for `T_last`. Same rule as today: one notification per restored episode.
- Startup: after a flagged stream is admitted to realtime, if its latest committed candle has no OI, the completer fills the tail
  and enqueues one notification for the latest committed bar. This is how an interrupted publication is finished after a restart,
  without a durable outbox.
- The OI history track keeps owning holes before the tail; duplicates between the two writers are classified `duplicate` by the
  existing importer.

### D5. Read API shape (owner choice)
- **A (recommended):** opt-in. `/v1/candles` and `/v1/historical-candles` accept `include_open_interest=true` for flagged streams;
  then every candle has `open_interest` (decimal string) and the response has `open_interest_hash`. Without the parameter the
  response is byte-identical to today. The Engine client opts in after its own change; nothing breaks on deploy.
- **B:** always include `open_interest` for flagged streams. Breaks the current Engine client on SOL the moment MDS is deployed.

For flagged streams, with or without the parameter, the served window is the joint window (D7), so every bar a consumer receives
is a published bar.

### D6. Hashes (owner choice)
- **A (recommended):** `market_data_hash` stays OHLCV-only and unchanged for every stream; OI gets its own
  `open_interest_hash` = SHA-256 of ticker, timeframe, range and ordered `(open_time_ms, open_interest)` pairs. All stored research and
  Engine hashes keep replaying.
- **B:** fold OI into `market_data_hash` for flagged streams. Every stored SOL hash becomes `coverage_stale`.

`/v1/historical-candles` with `include_open_interest=true` also accepts `expected_open_interest_hash`.

### D7. Joint window for flagged streams
`available_from = max(candle earliest, OI lower bound)`, `available_to = min(latest committed candle, latest OI) + step`. Ranges
outside it are rejected as today (`range_unavailable`); a hole in either track inside the range fails the complete-grid rule. Candles
of a flagged stream older than its OI lower bound are not served on `/v1/candles` (e.g. BTC before 2020-08-04).

### D8. Configuration
No new key. `open_interest = true` now means both "maintain OI history" and "publish only with OI". With the flag off the stream
behaves exactly as before this change, and stored OI rows stay untouched.

## Failure modes
- Bybit OI REST down for minutes: live bars of flagged streams are committed but not published; the completer retries; unflagged
  streams are unaffected. Logged per retry; no timeout that publishes without OI.
- Completer queue full: the item is dropped with an error log; the next restart or restored episode re-finds the tail (D4).
- Crash after OI stored, before send: restart finds OI present for the latest bar; the notification is sent once on startup.

## Rollout
1. Engine client change to read `include_open_interest` (separate Engine OpenSpec, only if the owner wants OI in strategies).
2. MDS with this change; SOL already has full OI history. BTC/ETH stay `open_interest = false` until their history is loaded with
   Runtime off.
