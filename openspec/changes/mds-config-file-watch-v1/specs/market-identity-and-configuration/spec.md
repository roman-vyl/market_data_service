## ADDED Requirements

### Requirement: Market configuration change pickup

When `MDS_CONFIG_WATCH_SECONDS` is positive, a running MDS SHALL check its market
configuration file at that interval. It SHALL validate any changed content with the
startup validation rules before acting on it, and SHALL act only on a valid change of the
enabled coverage, by stopping gracefully with exit code `75` so the process supervisor
restarts it with the new configuration. When the setting is `0` or absent, MDS SHALL
behave as before and read the file only at startup.

#### Scenario: Valid coverage change restarts MDS

- **WHEN** the watched file changes to a valid configuration whose enabled instruments,
  exchange symbols, timeframes or history policies differ from the running ones
- **THEN** MDS SHALL log the added, removed and changed streams
- **AND** SHALL stop through the same graceful path as a termination signal
- **AND** SHALL exit with code `75`.

#### Scenario: Change without coverage difference

- **WHEN** the watched file changes but its validated enabled coverage equals the
  running one
- **THEN** MDS SHALL keep running and log that the coverage is unchanged.

#### Scenario: Invalid edit is ignored

- **WHEN** the watched file becomes unreadable, missing or invalid
- **THEN** MDS SHALL log one error with the loader message
- **AND** SHALL keep running on the current configuration
- **AND** SHALL NOT report the same content again until the file changes.

#### Scenario: Watch disabled by default

- **WHEN** `MDS_CONFIG_WATCH_SECONDS` is unset or `0`
- **THEN** MDS SHALL NOT start a configuration watcher.
