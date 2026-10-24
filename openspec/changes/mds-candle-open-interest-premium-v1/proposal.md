# MDS: open interest and premium on the 5m candle v1

## Status
DRAFT for the owner's decision. No code until the owner's word.

## Owner's rule (2026-10-24 08:01)
OI is loaded the same way as always and served with the same candles, as a parameter, like volume: volume is not a candle
either, but it is stored in the database with it. We already load 5m.

## Why
Vote W8 variant 6 (EMA200): nine voters read Bybit open interest (W8_30, W8_14, W8r, W8p, O1, kI, W1r, W1d, W2d), one reads
the Bybit premium index (PS_R). MDS stores only OHLCV today. The Engine reads its candles from MDS itself
(`strategy_engine/adapters/market_data_service/client.py`), so values that come with the candle reach the Engine with no change in
Research Service or Runtime.

## What changes
1. **Two new optional fields of the 5m candle row**: `open_interest` (Bybit OI at the bar open time) and `premium_close`
   (close of the Bybit premium-index 5m bar with the same open time). Nullable columns of `candles`, decimal text like OHLCV.
   Other timeframes and instruments keep them empty.
2. **Configuration.** An instrument enables them with `candle_extras = ["open_interest", "premium"]`. Now: BTC, ETH, SOL.
3. **Source.** The Bybit kline does not carry OI or premium, so MDS makes two more REST calls for the same 5m window:
   `GET /v5/market/open-interest` (`intervalTime=5min`, 200 per page) and `GET /v5/market/premium-index-price-kline`
   (`interval=5`, 1000 per page). History is loaded together with the candle windows (backfill fills existing rows); live, after
   each closed 5m bar.
4. **Read.** `/v1/candles` and `/v1/historical-candles` return the two fields (null when not loaded). The candle hash stays over
   OHLCV only, so experiments saved earlier keep their hash; the extras get their own `extras_hash` in the response.
5. **Readiness of the bar.** For an instrument with extras the 5m bar notification to Runtime waits for both values, at most
   `MDS_EXTRAS_NOTIFY_WAIT_SECONDS` (default 20) (owner choice 1/2 pending).

## Check before the Engine uses it
Research used OI with a 15-minute step. Bybit's 15-minute value at time T is expected to equal the 5-minute value at T; this is
checked once against the research files (`BBB_data/research/market_meta/oi_*_15min.csv` on the Mac, copied to the cloud).
Premium is checked against `/mnt/project-files/ema200_calib/market_meta_premium/`.

## Out of scope
Engine: reading the two fields and the voters (Engine thread). bbb_stack: config lines and image pin (separate PR).
