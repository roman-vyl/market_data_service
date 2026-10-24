## 1. Storage and config
- [ ] 1.1 `candle_extras` in market config (values `open_interest`, `premium`; 5m streams only), README
- [ ] 1.2 Schema v3: two nullable columns on `candles`, `extras_latest_open_time_ms` in `stream_state`; migration v2 -> v3

## 2. Bybit adapter
- [ ] 2.1 REST: open-interest (5min, cursor paging) and premium-index-price-kline; parsers with fixture tests; same retry policy

## 3. Ingestion
- [ ] 3.1 `AttachCandleExtras` use case (history, repair, live share it)
- [ ] 3.2 Backfill fills extras of existing rows; audit reports bars without extras
- [ ] 3.3 Live: separate step after each committed 5m bar; candle WS/REST/recovery/notification code untouched (regression tests stay green)

## 4. Read
- [ ] 4.1 `/v1/candles` and `/v1/historical-candles` return the fields and `extras_hash`; OpenAPI

## 5. Checks
- [ ] 5.1 Targeted unit tests
- [ ] 5.2 Parity with research files (OI 15m = 5m at the same time; premium)
