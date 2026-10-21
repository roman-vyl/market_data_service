## ADDED Requirements

### Requirement: Websocket connection diagnostics
The service SHALL log every websocket connect, disconnect (with close code and reason), transport failure and reconnect exhaustion, and SHALL warn when the event loop was blocked longer than `MDS_EVENT_LOOP_LAG_WARN_SECONDS`.

#### Scenario: Websocket closes
- **WHEN** the websocket closes
- **THEN** a warning containing the close code and reason is logged

### Requirement: Configurable keepalive timeout
The websocket keepalive pong timeout SHALL be configurable through `MDS_WS_PING_TIMEOUT_SECONDS` and default to 60 seconds.

#### Scenario: Default
- **WHEN** the variable is not set
- **THEN** the timeout is 60 seconds
