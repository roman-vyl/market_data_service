# MDS: open interest as a field of the 5m candle v1

## Status
PROPOSAL for the owner. No code until the owner's word.

## Why
Nine voters of vote W8 variant 6 (EMA200) read Bybit open interest (OI). MDS stores only OHLCV. The Engine reads its candles
from MDS, so OI must become part of the candle MDS serves.

Check 2026-10-24 (`/mnt/project-files/ema200_calib/market_meta_check/oi_5m_vs_15m.py`): Bybit 5-minute OI at :00/:15/:30/:45
equals the 15-minute series used in research exactly (BTC 189,079 of 189,086; the 7 others are one glitch of the 15-minute
series on 2022-01-19; ETH and SOL all equal). The 5-minute series has no gaps.

## What changes
For an instrument with `open_interest = true`, its 5m candle has one more field, `open_interest`: the Bybit 5-minute OI whose
timestamp equals the candle `open_time_ms`. From the configured `open_interest_from` on, a 5m candle of such an instrument is
complete (committed, counted as present, served, notified) only with its OI, exactly as it is only complete with its volume.

- Existing owners fetch OI next to OHLCV: the realtime handler for the WebSocket candle, the Bybit REST source for every REST
  window (backfill, gap repair, recovery). A bar whose OI is not yet available is not committed; the existing expected-bar
  recovery, retry and gap repair fetch it again.
- Notification code is unchanged: it fires on commit, and an OI-enabled bar is committed only with its OI.
- OI is never forward-filled, never taken from another timestamp, never served stale.
- Existing 5m rows without OI count as missing bars for OI instruments; the existing audit and gap repair fill them.
- Instruments without `open_interest`, and all other timeframes, behave exactly as today.
- `market_data_hash` stays over OHLCV.

## Out of scope
Research Service, Runtime, Engine (reading the field is the Engine thread's work), premium index, bbb_stack config and pin.
