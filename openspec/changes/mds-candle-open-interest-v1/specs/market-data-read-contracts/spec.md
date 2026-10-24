## ADDED Requirements

### Requirement: Candle reads carry open interest for OI-enabled 5m streams
`/v1/candles` and `/v1/historical-candles` SHALL return `open_interest` on each candle of an OI-enabled 5m stream (null before
`open_interest_from`). Responses of other streams SHALL NOT change. `market_data_hash` SHALL stay computed over OHLCV only.

#### Scenario: Experiment saved before OI was enabled
- **WHEN** an experiment read a BTC 5m range before OI was enabled
- **THEN** a later hash-bound historical read of that range returns the same `market_data_hash`.
