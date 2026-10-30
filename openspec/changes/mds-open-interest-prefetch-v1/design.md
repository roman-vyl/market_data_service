## D1. Prefetch stores the point and nothing else
The task calls the existing `AppendLiveOpenInterest.execute(stream, T)`, which already returns without a request when the point is stored and otherwise imports exactly `[T, T+step)` through `ImportOpenInterestWindow`. It does **not** report to the supervisor. The open interest continuity frontier keeps moving exactly as today: the publication step (`OpenInterestPublication.prepare`) finds the point stored, reports `observe_open_interest(stream, T)` (one step from the previous point) and checks `published_through(T)`. So the "frontier moves by one step on a live append, a larger jump raises a discontinuity" rule is untouched and a prefetched point can never advance the frontier ahead of a committed candle.

## D2. Which bar, which streams, when
Every second (same cadence as the stale worker) for each stream that has the open interest flag, is admitted, has an active subscription and no pending recovery: `T = align(now, step)`, the bar open now. If `now >= T + prefetch_delay` and the stream's in-memory attempt state for T says the point is not stored and the next attempt is due, run one attempt. Memory per stream: `(open_time_ms, stored, next_attempt_at, attempts)`; nothing is persisted, a restart re-reads storage because `execute` checks it first.

Streams in recovery are skipped: the recovery episode repairs both tracks over its window, so a prefetched point ahead of the lower frontier would only add noise.

## D3. Retry until the bar closes
Not yet published (empty answer) or failed attempt: next attempt after `retry_interval` (15 s). Attempts stop when the point is stored or bar T closes (`now >= T + step`). After close the existing path owns the bar. No exponential backoff: one request per stream per 15 s is at most 18 requests per bar per stream when Bybit is down, far below Bybit limits, and each attempt is bounded by `oi_live_timeout_seconds` (2 s).

## D4. Concurrency and isolation
Attempts run via `asyncio.to_thread` under a semaphore (default 4). They do not take the operation gate: the live open interest append at commit already runs without it (spec: "Live open interest is fetched when its candle is committed"). They never run inside the WebSocket receive path. A duplicate of the same point (prefetch racing the commit-time fallback) is classified `DUPLICATE` by the importer and is harmless. On shutdown the task stops creating attempts; an in-flight thread is allowed to finish like a notification send.

## D5. Fallback is the existing path
At commit the publication step runs unchanged. If the point is stored it sends at once; if not (Bybit down during the whole interval, prefetch off, restart in the middle of the bar) it performs its existing single attempt and, on failure, leaves the bar to the recovery episode. No new failure mode.

## D6. Observability
One INFO line when a point is stored by prefetch: stream, open time, seconds after bar open, attempts. One WARNING when bar T closes with the point still missing: stream, open time, attempts, last error. This gives the before/after measurement without extra tooling.

## Risks and checks
- **Point beyond the candle frontier.** The stored point may sit one step ahead of the committed candle for up to five minutes. To check in tasks: historical and recovery open interest audits and the track progress (`latest_committed_open_time_ms`) tolerate a point beyond the candle frontier; read paths expose open interest only up to the candle bounds.
- **Bybit publishes later than expected.** Handled by D3; delay is configurable. If the snapshot ever changed after first publication the importer would classify `CORRECTED` and store the correction (same as today).
- **Thread pool pressure.** Four threads at most, each bounded by 2 s; the notification worker and recovery use `to_thread` too, so the default executor size is checked in tasks.

## Alternatives rejected
- Fetch on the first WebSocket message of bar T: couples open interest to the feed that arrives late.
- Prefetch inside the notification worker: keeps the serial queue.
- Ticker WebSocket `openInterest`: a live value, not the 5m snapshot; equality with the snapshot is not established.

## Acceptance (after deploy, on a day of production logs)
Compared with the baseline of 2026-10-29/30: median Runtime receipt delay on the WebSocket path from 7.2 s toward the WebSocket latency alone, median first-to-last spread of one bar from 8 s toward 1-2 s, bars unpublished because of open interest from 59 per 26 h toward 0. Bars delivered by REST recovery are expected to stay (separate change).
