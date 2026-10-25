## ADDED Requirements

### Requirement: Candle ingestion is independent of open interest
Realtime and REST candle ingestion, classification, recovery, the single notification of a restored episode and the stream lifecycle SHALL NOT depend on open interest. A confirmed valid candle of a flagged stream SHALL be committed whether or not its open interest is available; only sending its notification waits for the open interest check.

#### Scenario: Bybit open interest endpoint is down
- **WHEN** open interest requests fail for a flagged stream
- **THEN** its candles keep being committed and recovered exactly as for an unflagged stream, and only their notifications are held
