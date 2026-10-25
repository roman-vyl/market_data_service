## ADDED Requirements

### Requirement: Open interest track is enabled per instrument
An instrument MAY declare the optional boolean `open_interest`, default `false`. When `true`, MDS SHALL enable the open interest track for that instrument's `5m` stream only. An instrument that declares `open_interest = true` without `5m` in `canonical_timeframes` SHALL be rejected at configuration loading. Instruments without the flag SHALL behave exactly as before.

#### Scenario: Flag on a 5m instrument
- **WHEN** an enabled instrument declares `open_interest = true` and `5m` in `canonical_timeframes`
- **THEN** its `5m` stream has an open interest track and its other timeframes do not

#### Scenario: Flag without 5m
- **WHEN** an instrument declares `open_interest = true` without `5m`
- **THEN** configuration loading fails before runtime work starts

#### Scenario: Flag absent
- **WHEN** an instrument does not declare `open_interest`
- **THEN** no open interest work runs for it and its streams behave as before
