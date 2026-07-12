# Tasks: WebSocket Realtime Recovery v1

- [ ] Define the realtime observation and connection-event port contracts.
- [ ] Add the Bybit public WebSocket adapter.
- [ ] Add deterministic multi-symbol subscription wiring from validated configuration.
- [ ] Parse and validate confirmed closed candle events.
- [ ] Ignore non-confirmed updates for canonical persistence.
- [ ] Route confirmed closes through `IngestObservedCandle`.
- [ ] Add duplicate/correction parity tests between REST and WebSocket observations.
- [ ] Add bounded reconnect backoff and cancellation.
- [ ] Add per-stream stale detection.
- [ ] Add reconnect catch-up by composing existing audit and repair use cases.
- [ ] Gate readiness until catch-up and freshness are proven.
- [ ] Add disconnect/reconnect/multi-stream isolation integration tests.
- [ ] Add a real Bybit WebSocket smoke without consumer dependencies.
- [ ] Update README, acceptance matrix, and base task statuses.
