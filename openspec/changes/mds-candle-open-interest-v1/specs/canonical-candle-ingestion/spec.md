## ADDED Requirements

### Requirement: An open-interest 5m candle is complete only with its own OI
For an OI-enabled 5m stream at or after `open_interest_from`, a candle SHALL be committed, counted as present, served and
notified only together with the Bybit 5-minute open interest whose timestamp equals its `open_time_ms`. MDS SHALL NOT
forward-fill OI, use OI of another timestamp, or commit such a candle without OI.

#### Scenario: OI not yet published at WebSocket confirmation
- **WHEN** the confirmed WebSocket candle for 10:00 arrives and Bybit has no 10:00 OI point yet
- **THEN** nothing is committed for 10:00, and the existing expected-bar recovery later commits it with its OI and notifies once.

#### Scenario: Existing history without OI
- **WHEN** OI is enabled for an instrument whose 5m rows have no OI
- **THEN** those rows count as missing bars and the existing gap repair fills them with OI without changing OHLCV.

### Requirement: Existing owners fetch OI next to OHLCV
OI SHALL be fetched by the existing realtime candle handler and by the existing Bybit REST candle source, and SHALL go through
the existing canonical ingestion, retry, recovery and repair. No separate OI lifecycle, state or worker SHALL be introduced.

#### Scenario: REST window with the last OI point missing
- **WHEN** a REST window returns klines for 10:00-10:55 and OI for 10:00-10:50
- **THEN** 10:55 is treated as a missing bar by the existing logic and fetched again.
