## ADDED Requirements

### Requirement: Candle commit is independent of open interest
Realtime and REST candle ingestion, validation and the `COMMITTED` classification SHALL NOT depend on open interest, and the WebSocket receive path SHALL NOT wait for an open interest request. A confirmed valid candle of a flagged stream SHALL be committed whether or not its open interest is available.

#### Scenario: Bybit open interest endpoint is down
- **WHEN** open interest requests fail for a flagged stream
- **THEN** its candles keep being committed as `COMMITTED`, and only the stream's readiness and publication reflect the missing open interest

### Requirement: Each data track has a proven continuity frontier
For a flagged stream the realtime supervisor SHALL keep an open interest continuity frontier next to the candle frontier. A frontier SHALL move only to the end of an audited continuous range or by exactly one step on a live append; a larger jump SHALL raise a sequence discontinuity. The latest stored point SHALL NOT prove continuity. Stale detection SHALL use the lower frontier.

#### Scenario: Partial open interest answer
- **WHEN** open interest at T+1 is stored but T is missing
- **THEN** the open interest frontier stays before T and the stream goes to recovery

### Requirement: Live open interest is fetched when its candle is committed
For a flagged stream, after candle T is committed MDS SHALL fetch open interest for exactly T by REST off the event loop, without the shared operation gate, with one attempt and a short timeout, store it through the existing open interest importer and report the result to the supervisor. A failed fetch SHALL NOT be retried by its own schedule; the expected-bar rule SHALL send the stream to the existing recovery.

#### Scenario: Open interest fetched in time
- **WHEN** candle T of a ready flagged stream is committed and Bybit returns open interest at T
- **THEN** the open interest frontier advances to T and the stream stays ready

### Requirement: Realtime recovery repairs both tracks over one window
For a flagged stream a realtime recovery episode SHALL repair candles and open interest with the existing gap repair over the same window, starting no later than the lower frontier plus one step, and SHALL end `restored` only when both repairs complete with continuous post-audits. Open interest points committed by the episode SHALL count as committed bars for the episode's single notification.

#### Scenario: Restart after a crash between candle and open interest
- **WHEN** MDS stops after committing candle T and before storing open interest at T
- **THEN** after restart the recovery window includes T, open interest at T is stored, the stream becomes ready and one notification is sent
