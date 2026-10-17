## ADDED Requirements

### Requirement: External market configuration file

MDS SHALL read its market configuration from the path given by `MDS_MARKETS_CONFIG_PATH`,
which MAY point outside the image, and SHALL fail at startup with its config-not-found
error when that file does not exist, without falling back to the configuration baked into
the image.

#### Scenario: Live stack uses the host file

- **WHEN** `MDS_MARKETS_CONFIG_PATH` points to a mounted host file
- **THEN** MDS SHALL load, validate and use that file
- **AND** SHALL NOT read the baked `config/markets.toml`.

#### Scenario: Missing external file

- **WHEN** the configured file does not exist at startup
- **THEN** MDS SHALL fail startup with the config-not-found error.

### Requirement: Market configuration change pickup

When `MDS_CONFIG_WATCH_SECONDS` is positive, a running MDS SHALL check its market
configuration file at that interval. It SHALL validate any changed content with the
startup validation rules before acting on it, and SHALL act only on a valid change of the
enabled coverage, by stopping gracefully with exit code `75` so the process supervisor
restarts it with the new configuration. When the setting is `0` or absent, MDS SHALL read
the file only at startup.

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
