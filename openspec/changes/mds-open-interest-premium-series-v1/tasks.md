## 1. Configuration and storage
- [ ] 1.1 `market_series` in market config: allowed values, duplicates rejected, default empty; README
- [ ] 1.2 Schema v3: `series_streams`, `series_points`, `series_stream_state`; migration v2 -> v3; repositories
- [ ] 1.3 Domain: series kinds with grid step (OI 15m, premium 5m), point validation, decimal policy

## 2. Bybit adapter
- [ ] 2.1 REST client: open-interest (cursor paging) and premium-index-price-kline; parsers with fixture tests
- [ ] 2.2 Same retry and rate-limit policy as klines

## 3. History and repair
- [ ] 3.1 `IngestSeriesPoints` use case (one path for history, repair, live)
- [ ] 3.2 Lower-bound discovery, bounded windows, resumable progress, gap audit for series streams
- [ ] 3.3 Runtime reconciliation includes series streams; readiness reports them

## 4. Live
- [ ] 4.1 Polling worker per series kind after bucket close + grace
- [ ] 4.2 Notification hold for instruments with series (D5, after the owner's choice)

## 5. Read API
- [ ] 5.1 `GET /v1/series`, `GET /v1/series-streams/{ticker}/{series}/bounds`, error envelope, OpenAPI

## 6. Checks
- [ ] 6.1 Targeted unit tests per task
- [ ] 6.2 Parity with research files (premium in cloud, OI after copy from Mac)
