## MODIFIED Requirements

### Requirement: Live open interest is fetched when its candle is committed
For a flagged stream, when candle T is committed and open interest T is not yet stored (not stored by the prefetch requirement below), MDS SHALL fetch open interest for exactly T by REST off the event loop, without the shared operation gate, with one attempt and a short timeout, store it through the existing open interest importer and report the result to the supervisor. When open interest T is already stored, MDS SHALL NOT send a request and SHALL report the stored point to the supervisor as a live append. A failed fetch at commit SHALL NOT be retried by its own schedule; the expected-bar rule SHALL send the stream to the existing recovery.

#### Scenario: Open interest fetched in time
- **WHEN** candle T of a ready flagged stream is committed and Bybit returns open interest at T
- **THEN** the open interest frontier advances to T and the stream stays ready

#### Scenario: Open interest already stored by prefetch
- **WHEN** candle T of a ready flagged stream is committed and open interest at T is already stored
- **THEN** no open interest request is sent and the open interest frontier advances to T

## ADDED Requirements

### Requirement: Open interest is prefetched during the bar's interval
For a stream with the open interest flag that is admitted, has an active subscription and no pending recovery, MDS SHALL, once a configurable delay (default 20 s; 0 disables) has passed since bar T opened, fetch open interest for exactly T by REST off the event loop and store it through the existing open interest importer, if it is not stored. While it is not stored, MDS SHALL repeat the attempt at a configurable interval (default 15 s) until it is stored or bar T closes. Attempts SHALL be bounded in concurrency (default 4), SHALL use the live open interest timeout, SHALL NOT take the shared operation gate, SHALL NOT run on the WebSocket receive path and SHALL NOT report to the supervisor; the open interest continuity frontier SHALL keep moving only through the live append at commit and through recovery. A prefetched point SHALL NOT be fetched again and SHALL NOT change candle ingestion, readiness or the notification contract. MDS SHALL log each prefetch stored (stream, open time, seconds after bar open, attempts) and each bar that closed without its open interest stored (stream, open time, attempts, last error).

#### Scenario: Normal live bar
- **WHEN** Bybit publishes open interest T 15 s after bar T opens
- **THEN** it is stored by the first attempt after the delay, and when candle T is committed the notification is sent without any open interest request

#### Scenario: Snapshot not published yet or request fails
- **WHEN** the first attempts return nothing or time out
- **THEN** the attempts repeat every retry interval until the point is stored or bar T closes

#### Scenario: Never available during the interval
- **WHEN** bar T closes with open interest T still missing
- **THEN** one warning is logged and the commit-time fetch and the existing recovery handle the bar exactly as without prefetch

#### Scenario: Stream under recovery
- **WHEN** a flagged stream has a pending recovery
- **THEN** no prefetch attempt is made for it

#### Scenario: Many streams due together
- **WHEN** ten flagged streams are due in the same second
- **THEN** no more than the configured number of requests are in flight and none holds the operation gate

#### Scenario: Prefetch disabled
- **WHEN** the delay is 0
- **THEN** behaviour is identical to the behaviour before this change
