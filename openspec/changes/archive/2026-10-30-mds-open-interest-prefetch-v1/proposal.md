# MDS open interest prefetch v1

## Why
Bybit publishes the open interest snapshot labelled T (5m) about 10-20 s after bar T **opens**, and it does not change afterwards (measured 2026-10-25). MDS fetches it only after candle T is committed, i.e. up to five minutes later, inside the publication path:

1. Candle T is committed and its notification is queued.
2. The single notification worker takes the queue one stream at a time. For each flagged stream it first requests open interest T from Bybit (about 1 s, timeout 2 s, one attempt) and only then sends the notification.
3. The tenth stream waits for nine other requests. A TLS handshake timeout on the one attempt (45 times in 26 h on 2026-10-29/30, all `handshake operation timed out`) leaves the bar unpublished (59 bars in 26 h); only REST recovery later delivers it, 24 s median instead of 7 s.

Measured over 3061 5m bars (Runtime receipt minus bar close): median 14 s; spread between the first and the last stream of one bar median 8 s; bars delivered by WebSocket path median 7.2 s, by REST recovery path median 24 s.

## What changes
A small background task fetches open interest T for every flagged 5m stream shortly after bar T opens and stores it through the existing importer. If Bybit has not published it yet or the request fails, the task retries until bar T closes. At close the existing publication path finds the point stored, skips REST and notifies at once.

- New settings: prefetch delay after bar open (default 20 s, 0 = off), retry interval (default 15 s), concurrency (default 4).
- Runs off the event loop, without the shared operation gate, never on the WebSocket receive path.
- The fetch at commit stays as the fallback when the point is still missing.

## Unchanged
Candle ingestion, the WebSocket path, the operation gate and its users, recovery planning and windows, the stale rule and grace, readiness and continuity frontiers (only the existing publication step moves them), the notification payload, queue and worker, historical reconciliation, Runtime and Engine.

## Out of scope
- Global operation gate granularity and parallel recovery across streams.
- Synchronous candle write on the WebSocket loop.
- The 15 s stale grace and recovery latency.
- Any open interest source other than REST `/v5/market/open-interest` (no ticker WebSocket).
