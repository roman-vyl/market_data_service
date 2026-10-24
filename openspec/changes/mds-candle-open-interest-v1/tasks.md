## 0. Before code
- [ ] 0.1 Measure the Bybit 5-minute OI publication delay on the Mac (1 hour, BTC/ETH/SOL); set the timeout (D6)
- [ ] 0.2 Owner chooses D5 (A or B)

## 1. Config and storage
- [ ] 1.1 `open_interest = true` per instrument (5m stream only), validation, README
- [ ] 1.2 Schema v3: `candles.open_interest_value`, `stream_state.oi_latest_open_time_ms`; migration v2 -> v3; repositories

## 2. Bybit adapter
- [ ] 2.1 REST `open-interest` (intervalTime=5min, 200 per page, cursor), parser, fixture tests; existing retry policy

## 3. Ingestion
- [ ] 3.1 `AttachOpenInterest` use case (history, repair, live share it)
- [ ] 3.2 OI history pass and gap audit (D4)
- [ ] 3.3 Live attach after each committed 5m bar of an OI instrument

## 4. Completeness and notification
- [ ] 4.1 Notification of OI instruments after the bar's OI is written (D3); timeout behaviour per D5
- [ ] 4.2 Regression: WebSocket, REST backfill, recovery and notification tests of instruments without OI unchanged and green

## 5. Read
- [ ] 5.1 `open_interest` in `/v1/candles` and `/v1/historical-candles`, `open_interest_hash`; OpenAPI

## 6. Checks
- [ ] 6.1 Targeted unit tests per task
- [ ] 6.2 Parity: MDS OI equals `/mnt/project-files/ema200_calib/market_meta/oi_*_5min.csv` on every bar
