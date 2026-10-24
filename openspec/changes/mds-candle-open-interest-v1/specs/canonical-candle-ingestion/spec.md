## ADDED Requirements

### Requirement: Open interest is a field of the 5m candle
For an instrument with `open_interest = true`, each 5m candle row SHALL carry the Bybit 5-minute open interest whose timestamp
equals the bar open time, as an exact decimal. Other instruments and timeframes SHALL leave the field empty.

#### Scenario: Instrument without open interest
- **WHEN** an instrument does not enable `open_interest`
- **THEN** its candles, reads, hashes and notifications are exactly as before.

### Requirement: Writing open interest never changes OHLCV
Open interest SHALL be written only into an existing candle row through one use case shared by history, repair and live, in
one transaction with `oi_latest_open_time_ms`. It SHALL NOT create a candle or change OHLCV, and the kline WebSocket and REST
ingestion SHALL keep their behaviour.

#### Scenario: OI for a bar without a candle
- **WHEN** Bybit returns an OI point for a bar that MDS has not committed yet
- **THEN** the point is not stored and is fetched again after the candle is committed.
