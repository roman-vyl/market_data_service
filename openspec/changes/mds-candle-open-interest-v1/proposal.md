# MDS: open interest on the 5m candle v1

## Status
PROPOSAL for the owner. No code until the owner's word.

## Owner's rules (2026-10-24)
- OI is loaded the way MDS always loads data and is served with the same candles, as a field, like volume (08:01).
- Nothing in the WebSocket, REST or any other existing mechanism is broken (08:02).
- MDS knows nothing about strategies; no separate cycle that waits for something (08:16).
- A bar is complete only together with its OI, as one record; data of different moments must never mix (08:14).

## Why
Nine voters of vote W8 variant 6 (EMA200) read Bybit open interest. MDS stores only OHLCV. The Engine reads candles from MDS
itself (live and batches), so an OI field on the candle reaches it with no change in Research Service or Runtime.

Check done 2026-10-24 (`/mnt/project-files/ema200_calib/market_meta_check/oi_5m_vs_15m.py`): Bybit OI with a 5-minute step at
:00/:15/:30/:45 equals the 15-minute series used in research exactly (BTC 189,079 of 189,086 points, the 7 others are one
glitch of the 15-minute series on 2022-01-19; ETH and SOL all equal). The 5-minute series has no gaps. So the 5m step is used.

## What changes
1. **Config.** An instrument enables `open_interest = true`. Only its `5m` stream gets OI. Now: BTC, ETH, SOL.
2. **Storage.** `candles` gets a nullable column `open_interest_value` (decimal text like volume); `stream_state` gets
   `oi_latest_open_time_ms` (last bar whose OI is stored). Schema v3, migration from v2; existing rows untouched.
3. **Value.** The OI of bar `T` is the Bybit point `GET /v5/market/open-interest?intervalTime=5min` with `timestamp = T`.
4. **History.** A finished-history pass, like candle backfill: windows of 200 bars, resumable, gap audit. 2021-06 to now is about
   567k points and 2,840 requests per coin (SOL from 2021-10-14, Bybit has no earlier 5m OI).
5. **Live.** Existing candle commit by WebSocket or REST stays exactly as it is. Right after a 5m bar of an OI instrument is
   committed, MDS asks Bybit for that bar's OI (REST, existing retry policy) and writes it into the same row.
6. **Bar complete = OHLCV + OI.** For an OI instrument the bar notification to Runtime is sent when the bar is complete, i.e.
   after its OI is written, not right after the candle commit. Instruments without OI: unchanged. This is a data rule of MDS,
   not a strategy rule.
7. **If Bybit does not give the OI** within `MDS_OI_COMPLETE_TIMEOUT_SECONDS` (decided after measurement, task 0): owner's
   choice A or B (design D5).
8. **Read.** `/v1/candles` and `/v1/historical-candles` return `open_interest` for each bar (null where not stored). The
   existing `market_data_hash` stays over OHLCV, so saved experiments keep their hash; a separate `open_interest_hash` covers OI.

## Unchanged
WebSocket kline ingestion, REST kline backfill, repair, recovery, readiness of candle streams, notifications of instruments
without OI, all other timeframes.

## Out of scope
Premium index (same pattern later, if needed). Engine reading the field (Engine thread). bbb_stack config and pin (separate PR).
