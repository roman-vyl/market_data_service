# MDS open interest stream integration v1

## Status
Draft for owner review (2026-10-25). No code. Three decisions below (D3, D5, D6) need the owner's choice before apply.

## Why
Step 1 (`mds-open-interest-history-track-v1`, archived 2026-10-25) loads and maintains open interest (OI) history for flagged 5m
streams as an independent track. Consumers still see candles only, and a bar is published to Runtime as soon as its OHLCV is
committed, with or without OI.

This change makes OI a required part of a *published* bar of a flagged stream, by extending the existing publication contract
(the `CommittedBarNotificationWorker` -> Runtime chain and the `/v1` read API), not by adding a second readiness mechanism or a
second delivery channel.

Smoke facts this design relies on (SOL, 2026-10-25): 528 918 / 528 918 points, 0 gaps, 100 % equal to the research CSV; Bybit
publishes the OI point at T (snapshot at the bar's open) 3-17 s after T, so when bar T closes at T+5m, OI(T) is normally already
available.

## What changes
- **Two notions, one lifecycle.** `COMMITTED` keeps its meaning: OHLCV valid and stored. A new derived condition
  `READY_TO_PUBLISH(stream, T)` = `COMMITTED(T)` and, only when the stream has `open_interest = true`, OI stored for exactly the same
  `open_time_ms`. No new lifecycle state, no new table, no previous/next/nearest OI value.
- **Live completion.** For flagged streams a live `COMMITTED` outcome does not enqueue the notification directly. A small
  `OpenInterestCompleter` fetches OI(T) by REST (one window, off the event loop, without waiting for the shared operation gate),
  stores it through the existing OI importer, then enqueues the same notification into the same worker. WebSocket never waits.
- **Recovery and restart.** A restored recovery episode and the startup tail go through the same completer: OI is fetched for every
  committed bar after the stream's OI watermark, then one ordinary notification for the latest bar (existing restored-episode rule).
- **Read API.** For flagged streams `/v1/candles` and `/v1/historical-candles` serve only the joint window where every bar has both
  OHLCV and OI. OI values are returned on request (D5) and carry their own hash (D6); unflagged streams are unchanged byte for byte.
- **Readiness.** A flagged stream is ready only when its OI history track is complete (D3), using the existing blocking-reason
  mechanism of the runtime status store.

## Unchanged
WebSocket connector, candle handler, ingestion and `COMMITTED` semantics; candle repair and realtime recovery algorithms; the
notification payload, single bounded worker and one-attempt HTTP delivery; Runtime's webhook; the OI history track; every stream and
instrument without the flag; `market_data_hash` of OHLCV.

## Out of scope
Engine/Runtime code (the Engine MDS client is strict, `extra="forbid"`: it needs its own change before it asks for OI);
premium index; OI for timeframes other than 5m; automatic handling of OI holes that Bybit never fills (owner rule 2026-10-25: such
holes are fixed once by hand and recorded in `OI_MANUAL_FIXES.md`).
