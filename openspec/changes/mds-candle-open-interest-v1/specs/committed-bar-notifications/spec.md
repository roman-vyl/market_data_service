## ADDED Requirements

### Requirement: A bar of an open-interest instrument is complete with its own OI
For an instrument with `open_interest = true`, MDS SHALL enqueue the committed-bar notification of a 5m bar only after that
bar's open interest is written, or after `MDS_OI_COMPLETE_TIMEOUT_SECONDS` with the field left empty (per design D5). It SHALL
never complete a bar with the open interest of another bar.

#### Scenario: OI arrives a few seconds after the candle
- **WHEN** the 10:00 candle is committed at 10:05:01 and its OI is written at 10:05:04
- **THEN** one notification for 10:00 is enqueued at 10:05:04.

#### Scenario: Instrument without open interest
- **WHEN** an instrument without `open_interest` commits a bar
- **THEN** the notification is enqueued as today.
