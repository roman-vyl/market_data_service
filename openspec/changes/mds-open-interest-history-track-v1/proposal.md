# MDS open interest history track v1

## Status
Approved by the owner 2026-10-25 and implemented. Smoke on a copy of the production database passed (migration, automatic start, resume, candle readiness).

## Why
Some configured 5m streams need Bybit open interest (OI) as a second required market-data source, next to kline. The target model
(owner, 2026-10-25) is a set of required tracks per stream: kline stays the first track, OI becomes the second for configured 5m
streams, and canonical readiness and bar publication later become the result of their joint completeness.

That is split into two changes so that history, live, readiness and publication are not changed at once:

1. **This change:** OI as an independent historical data track (lower bound, bounded backfill, durable progress, audit, repair,
   restart) stored in the same production database. Production live keeps working exactly as today.
2. **Later change `mds-open-interest-stream-integration-v1`:** composite readiness, live acceptance of bar T only with OI(T),
   restart tail recovery and the read contract. Not designed here.

When step 2 is switched on, the history is already in the database and proven by audit; step 2 only maintains the tail.

The lower bounds are independent Bybit facts, measured on 2026-10-25 on the owner's Mac: 9 of 11 coins have OI within 0-2 h of
their first 5m candle, BTC has candles from 2020-03-25 but OI only from 2020-07-20 (117 days later), TRX has OI from 2021-08-17,
before its first MDS candle (2021-08-31).

## What changes
- Configuration: optional per-instrument flag `open_interest = true` enables the OI track for that instrument's 5m stream.
- Storage (schema v3): a new table of OI values per stream and `open_time_ms`, and a new table of per-stream track progress. The
  candle table and `stream_state` are not changed.
- Source: a new port and Bybit adapter for `/v5/market/open-interest` (`intervalTime=5min`, windows of at most 200 points), on the
  existing REST transport and retry.
- History: OI lower-bound discovery, audit-driven repair over `[oi_lower_bound, last closed bar]` and restart reconstruction,
  using the existing algorithms with two small parametrizations (where the data comes from, where progress is written), not
  copies of the classes.
- Runtime: the OI track runs automatically inside the running service for flagged streams, with no manual commands, in the background, under the existing
  REST operation gate, after candle startup, and keeps its tail up to date until step 2.

## Unchanged
`RealtimeConnector`, `RealtimeCandleHandler`, `RealtimeSupervisor`, realtime recovery, `CommittedBarNotificationWorker`, public
stream readiness and lifecycle, `/v1/candles` and other read contracts, Runtime notifications, the meaning of a COMMITTED candle,
the candle table, `stream_state`, candle startup reconciliation. Streams and instruments without the flag behave exactly as today.

## Out of scope
Everything in step 2; reading OI through the API; Research and Engine changes; premium index.
