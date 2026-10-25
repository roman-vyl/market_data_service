## MODIFIED Requirements

### Requirement: Open interest work shares REST serialization and keeps its tail
Every open interest history pass SHALL run under the same runtime operation gate as candle historical reconciliation and realtime REST recovery, with a finite per-pass window budget. After start, each flagged stream SHALL be reconciled over its full open interest window until one pass completes; the track SHALL then not start another cycle for that stream until the next service start. New open interest points after that SHALL come only from the committed-bar publication check, so there is one regular source of new points.

#### Scenario: Realtime recovery becomes due during open interest history
- **WHEN** a realtime REST recovery becomes due while an open interest pass holds the operation gate
- **THEN** the recovery runs after that one bounded pass, not after the whole open interest history

#### Scenario: Tail after completion
- **WHEN** the open interest history of a stream is complete and new bars close
- **THEN** the history track does not scan the history again, and the publication check stores the new points

#### Scenario: Backfill on start
- **WHEN** the service starts with a flagged stream whose open interest history has holes
- **THEN** the history track repairs them in bounded passes until the audit is complete
