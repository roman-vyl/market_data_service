# Design

## D1. Series are their own streams, not candles
A premium kline has no volume and an OI point is not a bar. Putting them into `candles` would bend the OHLCV validation
rules. Series get `series_streams(instrument_id, series)` and `series_points(series_stream_id, time_ms, value, open, high,
low, source, committed_at_ms)`; `open/high/low` are filled for premium only. `stream_state` is reused through a
`series_stream_state` table with the same columns and states.

## D2. One ingestion path per series
REST history, repair and live polling all go through one use case `IngestSeriesPoints`: payload -> adapter normalisation ->
validation (time on grid, decimal value, OI >= 0) -> classification (new / duplicate / conflicting) -> atomic commit of the
points with their `series_stream_state`. Conflicting values from REST replace older REST values (REST is authoritative),
same as candles.

## D3. Time stamps
- Bybit OI `timestamp` is the start of the 15-minute bucket; stored as is. Grid check: `time_ms % 900000 == 0`.
- Premium kline `startTime` is the bar open; stored as is. Grid check: `time_ms % 300000 == 0`.
The as-of rule (which point a 5m bar may use) belongs to the Engine and is not applied in MDS.

## D4. Live polling
A small worker per series kind: after `bucket_close + MDS_SERIES_POLL_GRACE_SECONDS` (default 5) it fetches the last 3
buckets for every series stream, ingests them, and retries with the existing REST backoff. Gaps found are repaired by the
same windows as history. Load: 3 coins x (12 + 4) requests per hour.

## D5. Notification hold (owner decision)
Option A (recommended): hold the 5m notification up to 20 s until the series points are in. Runtime and Engine stay
unchanged; a late series costs at most 20 s of latency on that bar.
Option B: notify at once; the Engine fails closed if the point is missing, and the W8 strategy skips that bar.

## D6. History volume
OI 15m since about 2021-06: about 185k points per coin, about 930 requests. Premium 5m since about 2020-10: about 630k bars per
coin, about 630 requests. Both a few minutes per coin with the existing rate-limit settings; total database growth about
150 MB for three coins.

## D7. Parity check before the Engine uses it
Compare MDS series with the research files: premium against
`/mnt/project-files/ema200_calib/market_meta_premium/premium_*_5min.csv` (in the cloud), OI against the Mac files
`BBB_data/research/market_meta/oi_*_15min.csv` (copied to the cloud first). Expected: identical values on every common time.
