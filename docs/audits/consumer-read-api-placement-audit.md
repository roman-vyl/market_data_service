# Consumer Read API v1 placement audit

## Existing paths reviewed

- `adapters/http/runtime_server.py`: owns the standard-library HTTP listener and current health/readiness route dispatch. It must remain transport composition only.
- `runtime/wiring.py`: builds concrete application dependencies. It is the only approved place to bind the consumer read use case to SQLite.
- `ports/storage.py`: transaction-oriented write/audit unit-of-work port. It already exposes `list_candles`, but adding HTTP/read-policy concerns here would broaden its responsibility.
- `adapters/sqlite/candle_repository.py`: canonical SQL range implementation already provides half-open, ascending reads. Consumer reads must reuse it rather than introduce different SQL.
- `adapters/sqlite/stream_state_repository.py`: canonical lifecycle snapshot and available-boundary source.

## Approved module map

```text
application/consumer_read/
  errors.py
  models.py
  validation.py
  get_candle_range.py
ports/consumer_read.py
adapters/sqlite/consumer_candle_reader.py
adapters/http/consumer_read/
  handler.py
  parsing.py
  serialization.py
  exception_mapping.py
runtime/wiring.py
adapters/http/runtime_server.py
```

## Dependency direction

```text
HTTP adapter -> application query -> consumer read port <- SQLite adapter
runtime wiring binds concrete implementations
```

The application package imports neither HTTP nor SQLite. The SQLite adapter imports neither HTTP schemas nor runtime orchestration. `runtime_server.py` only delegates `/v1/candles` to the focused handler. Existing candle SQL remains in `SqliteCandleRepository.list_range`.
