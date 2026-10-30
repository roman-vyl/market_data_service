## ADDED Requirements

### Requirement: Prefetch accepts the open bar's open interest point
The open interest read policy SHALL have two admission rules for the time of a returned point: `closed_only`, under which only points of closed bars are accepted (history, recovery, lower-bound probing and the fetch at commit), and `include_current`, under which the point of the one bar open at the observation time is accepted too. A point of a bar later than the open bar SHALL be rejected under both rules. The prefetch SHALL use `include_current` and no other path SHALL. All other parsing, validation, de-duplication, classification and storage SHALL be the same code for both rules.

#### Scenario: Prefetch stores the open bar's point
- **WHEN** Bybit has published open interest T and MDS asks for exactly bar T while bar T is open
- **THEN** the point is stored by the existing importer and the attempt counts as stored

#### Scenario: Other paths stay closed-only
- **WHEN** history, recovery, lower-bound probing or the fetch at commit receives a point of a bar that is not closed
- **THEN** the point is discarded as before

#### Scenario: Future point
- **WHEN** an answer contains a point labelled later than the bar open at the observation time
- **THEN** it is rejected under both rules
