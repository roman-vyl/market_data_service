## ADDED Requirements

### Requirement: Readiness of an open-interest stream covers both data tracks
A stream whose instrument has `open_interest = true` SHALL be ready only when its candle track and its open interest track are both proven continuous from their lower bounds, both are current up to the expected bar under the same expected-bar rule and grace, and no recovery of either track is pending or unfinished. The same condition SHALL decide publication. Unflagged streams keep the existing definition.

#### Scenario: Open interest behind after the grace
- **WHEN** a ready flagged stream has candle T but no open interest at T after the expected-bar grace
- **THEN** the stream leaves ready through the existing recovery and degraded path

#### Scenario: Open interest history not loaded
- **WHEN** a flagged stream starts with gaps in its open interest history
- **THEN** it is not ready until those gaps are repaired

#### Scenario: Both tracks current
- **WHEN** candle and open interest frontiers of a flagged stream are both at the expected bar and no recovery is pending
- **THEN** the stream is ready
