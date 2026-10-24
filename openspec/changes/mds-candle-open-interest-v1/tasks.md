## 1. Config, domain, storage
- [ ] 1.1 `open_interest` and `open_interest_from` per instrument (5m only), validation, README
- [ ] 1.2 Optional `open_interest` on `ObservedCandle`/`CanonicalCandle`; validation and classification compare OHLCV + OI (D2)
- [ ] 1.3 Schema v3: `candles.open_interest_value` nullable; migration v2 -> v3

## 2. Existing owners fetch OI
- [ ] 2.1 Bybit REST adapter: `open-interest` (intervalTime=5min, cursor paging), parser, fixture tests, existing retry policy
- [ ] 2.2 `fetch_closed_candles` joins OI for OI-enabled 5m streams; candles without OI are not returned (D4)
- [ ] 2.3 Realtime handler: OI point for the confirmed candle before ingestion; nothing committed if missing (D3)

## 3. Presence and reads
- [ ] 3.1 Repositories: for OI-enabled 5m at/after `open_interest_from`, only rows with OI count as present (audit, state, reads) (D5)
- [ ] 3.2 `open_interest` in `/v1/candles` and `/v1/historical-candles` for OI-enabled 5m streams; OpenAPI (D6)

## 4. Tests (targeted)
- [ ] 4.1 Instruments without OI and other timeframes: existing tests unchanged and green
- [ ] 4.2 WebSocket candle with OI available: one commit with OI, one notification
- [ ] 4.3 WebSocket candle with OI not yet available: no commit, no notification; expected-bar recovery commits it with OI, one notification
- [ ] 4.4 REST window with OI missing for the last bar: that bar is not committed; next pass commits it
- [ ] 4.5 Existing rows without OI become gaps and are repaired with OI; OHLCV and `market_data_hash` unchanged
- [ ] 4.6 No bar is ever served or notified with OI of another timestamp

## 5. Checks
- [ ] 5.1 Parity: MDS OI equals `/mnt/project-files/ema200_calib/market_meta/oi_*_5min.csv` on every bar
- [ ] 5.2 Mac measurement of the OI publication delay (D7)
