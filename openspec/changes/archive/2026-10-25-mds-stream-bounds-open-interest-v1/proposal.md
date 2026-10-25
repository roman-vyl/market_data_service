# MDS stream bounds with open interest v1

## Status
Draft for owner review (2026-10-25). No code until the owner's word.

## Why
For a stream whose instrument has `open_interest = true`, OHLCV and open interest (OI) are one mandatory data set: READY and
publication to Runtime already require both tracks (`mds-open-interest-stream-integration-v1`). The history depth reported by
`GET /v1/streams/{ticker}/{timeframe}/bounds` still comes from candles only: `GetStreamBounds.execute` returns
`get_committed_candle_bounds` = `MIN/MAX(open_time_ms)` of the candle table. A consumer that plans history from bounds can therefore
plan a range that has candles but no OI (before the OI lower bound, e.g. BTC: candles from 2020-03-25, OI from 2020-07-20), or a last
bar whose OI is not stored yet.

## What changes
Only the use case `GetStreamBounds`:
- **Stream without an OI track** (instrument without `open_interest = true`, and every non-5m stream of a flagged instrument): unchanged, bounds = committed candle bounds.
- **Stream with an OI track** (the 5m stream of a flagged instrument, `ValidatedMarketConfig.open_interest_streams`): bounds = intersection of the two tracks:
  - `earliest_committed_open_time_ms = max(candle earliest, OI earliest)`;
  - `latest_committed_open_time_ms = min(candle latest, OI latest)`.
- **OI track bounds** come from the existing durable track progress (`get_track_progress(stream, "open_interest")`):
  earliest = resolved OI lower bound (`earliest_available_open_time_ms`), latest = latest committed OI point
  (`latest_committed_open_time_ms`). No new storage method, table or query.
- **Invariant:** if either track has no bounds (no candles, OI lower bound not resolved, no OI point committed) or the intersection is
  empty (`earliest > latest`), both fields are `null`. Bounds of one track are never returned alone for a flagged stream.
- `state` is returned exactly as today.

## What does not change
- The endpoint stays read-only and readiness-independent: no readiness requirement, no lifecycle transition, no REST fetch, backfill or
  repair as a side effect.
- Bounds are bounds, not a continuity audit: the intersection does not prove that every bar inside it is present; continuity stays the
  job of `continuity-audits`, READY and `/v1/candles`. Candle bounds keep their current meaning (`MIN/MAX` of committed candles).
- Public DTO and contract version `market_stream_bounds.v1`, HTTP route, `/v1/candles`, `/v1/historical-candles`, continuity audits,
  `market_data_hash` and any other hash, READY, notifications, Runtime, Engine, Research.

## Impact
- Code: `application/history_planning/get_stream_bounds.py` (`GetStreamBounds`) and its wiring (`runtime/wiring.py` `stream_bounds`)
  to know which streams are flagged; narrow tests.
- Consumers (accepted by the owner 2026-10-25 20:12): for flagged streams the reported depth starts at the OI lower bound. Research
  `full_available` backtests and Engine episode history of BTC start 117 days later (2020-07-20 instead of 2020-03-25), also for
  strategies that do not read OI; for 9 of 11 coins the difference is 0-2 hours. Engine live-entry/open-trade get a target bar beyond
  `latest` (candle committed, OI not yet) refused by their existing `target_bar_not_committed` check.
- Hashes are out of scope; they are decided together with the OHLCV + OI candle contract (owner's preference: keep `market_data_hash`
  OHLCV-only and add a separate OI hash).
