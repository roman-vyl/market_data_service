# Design

## D1. Stored on the candle row
Schema v3 adds `open_interest_value TEXT NULL` and `premium_close_value TEXT NULL` to `candles`, plus
`extras_latest_open_time_ms` in `stream_state`. A row with OHLCV and no extras is a valid candle; extras are filled by an
update of the same row. OHLCV validation and correction rules are unchanged.

## D2. One path for extras
`AttachCandleExtras(stream, window)`: fetch OI and premium for the window, validate (on grid, decimal, OI >= 0), and in one
transaction write them to the existing candle rows and move `extras_latest_open_time_ms`. Used by history backfill, gap repair
and live. A value for a time with no candle row is kept until the candle arrives (next pass), never creates a candle.

## D3. Live
After a 5m candle is committed (WebSocket or REST), MDS runs `AttachCandleExtras` for the last 3 bars with the REST retry
policy, as a separate worker: it never blocks or changes candle ingestion, recovery or notification. Load: 3 coins x 2 requests every 5 minutes.

## D4. Hash
`market_data_hash` stays over OHLCV, so saved experiment hashes and the stale-provenance rule are unaffected. Responses add
`extras_hash` over `(open_time_ms, open_interest, premium_close)`; the Engine can bind to it when a strategy reads extras.

## D5. History volume
OI 5m since about 2021-06: about 560k points per coin, about 2,800 requests; premium 5m since 2020-10: about 630 requests per
coin. About 10-20 minutes per coin with the current rate limits; database growth about 60 MB for three coins.
