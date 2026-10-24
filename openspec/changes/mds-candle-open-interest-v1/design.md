# Design

## D1. Config
Per instrument: `open_interest = true` and `open_interest_from = "YYYY-MM-DD"` (UTC, on the 5m grid). Only the instrument's
`5m` stream is affected. Bars before `open_interest_from` keep an empty field and stay complete as today (Bybit has no 5m OI
there; for SOL 5m OI starts 2021-10-14). Now: BTC, ETH, SOL.

## D2. Domain and storage
`ObservedCandle` and `CanonicalCandle` get an optional `open_interest` (exact decimal, >= 0). Storage: one nullable column
`candles.open_interest_value` (schema v3, migration adds the column; existing rows untouched). Validation: for an OI-enabled
5m stream at or after `open_interest_from`, an observation without OI is not committable. Classification compares OHLCV and
OI: a REST observation that adds or changes OI of an existing row is a correction under the existing REST correction
authority. No new state, table or stream lifecycle.

## D3. WebSocket path (existing owner: realtime candle handler)
WebSocket ingestion is unchanged up to the confirmed candle. For an OI-enabled 5m stream the handler asks the Bybit REST
source for the OI point with the same `open_time_ms` (one request, off the event loop) and passes one observation with OHLCV
and OI to the existing ingestion. If the point is not available, the handler commits nothing for that bar. The existing
expected-bar check then sees a missing bar and starts the existing REST recovery (D4), with its existing retry schedule.

## D4. REST paths (existing owner: Bybit REST source)
`fetch_closed_candles` for an OI-enabled 5m stream also requests `GET /v5/market/open-interest` (`intervalTime=5min`) for the
same window and joins by timestamp. Candles of that window whose OI point is missing are not returned. Backfill, gap repair and
realtime recovery then treat them as missing exactly as they treat a missing kline today (`incomplete` recovery, gap, next
pass). Window sizes stay; an OI window of 200 points needs one or two OI pages per kline page of 1,000 (about 5 pages).

## D5. Presence, readiness, reads
For OI-enabled 5m streams at or after `open_interest_from`, a stored row without OI is not a present bar: continuity audit,
gap repair, `latest_committed_open_time_ms`, readiness and range reads all count only rows with OI. So the existing readiness
and read rules give "not ready / gap" until OI is there, with no extra state.

Consequence when OI is first enabled on a coin with existing history: its 5m rows from `open_interest_from` are gaps until the
existing gap repair fills them (about 570k bars, about 2,840 OI requests and 570 kline requests per coin). Until then that
coin's 5m reads fail closed as with any gap; other coins and timeframes are not affected. Enable at a deploy.

## D6. Read contract
`/v1/candles` and `/v1/historical-candles` add `open_interest` to each candle of an OI-enabled 5m stream (null before
`open_interest_from`). Responses of other streams do not change. `market_data_hash` stays over OHLCV, so experiments saved
earlier keep their hash. No separate OI hash in v1: OI is written only with the complete candle, so the existing provenance is
enough; an OI hash is added only if the Engine contract asks for it.

## D7. Check before deploy
One short Mac measurement: seconds between a 5m close and the moment Bybit returns that bar's 5-minute OI (BTC/ETH/SOL,
1 hour). It changes no setting; it tells how often a live bar goes through recovery instead of the WebSocket path, and so how
many seconds a bar of these coins arrives later.
