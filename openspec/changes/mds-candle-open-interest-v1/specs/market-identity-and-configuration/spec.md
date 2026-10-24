## ADDED Requirements

### Requirement: Open interest is configured per instrument
An enabled instrument MAY set `open_interest = true` with `open_interest_from` (a UTC date on the 5m grid). It SHALL affect only
the instrument's `5m` stream. Other instruments and timeframes SHALL behave exactly as without it.

#### Scenario: Instrument without open interest
- **WHEN** an instrument does not set `open_interest`
- **THEN** its ingestion, readiness, reads, hashes and notifications are unchanged.
