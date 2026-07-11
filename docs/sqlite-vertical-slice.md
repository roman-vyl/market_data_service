# SQLite Vertical Slice

## Scope

The first network-free production slice implements one canonical ingestion decision against schema v1.

It includes:

- schema creation and version validation;
- stream registration;
- canonical candle lookup and persistence;
- stream-state persistence;
- quarantine persistence;
- one atomic SQLite unit of work;
- application-level validation and classification.

It does not include Bybit REST, WebSocket, HTTP API, backfill orchestration, or gap repair.

## Module boundaries

```text
application/ingest.py
  validation, classification, correction policy, state advancement

ports/storage.py
  application-facing atomic storage contract

adapters/sqlite/connection.py
  connection and per-connection pragmas

adapters/sqlite/schema.py
  schema creation and version validation

adapters/sqlite/catalog_repository.py
  instrument/stream registration and ID resolution

adapters/sqlite/candle_repository.py
  canonical candle reads and writes

adapters/sqlite/stream_state_repository.py
  lifecycle snapshot reads and writes

adapters/sqlite/quarantine_repository.py
  durable rare-problem records

adapters/sqlite/transaction.py
  transaction ownership and repository composition
```

No repository performs domain validation or correction classification.

## Ingestion outcomes

### New candle

The candle is inserted and `latest_committed_open_time_ms` advances in the same transaction.

### Exact duplicate

No candle or state row is changed.

### REST correction

The difference is recorded in quarantine and REST replaces the canonical candle because REST is the repair authority.

### WebSocket correction

The difference is recorded in quarantine, but the existing canonical candle is not overwritten.

### Invalid or unconfirmed observation

The use case rejects the observation before opening a storage transaction.

### Unknown stream

The observation is rejected as unconfigured and no row is written.

## Atomicity

`SqliteUnitOfWork` starts `BEGIN IMMEDIATE` and owns commit/rollback.

If an exception escapes the context, candle and stream-state changes are rolled back together.

## Verified scenarios

The integration tests cover:

- schema creation;
- insert and readback;
- canonical Decimal text;
- duplicate idempotency;
- REST correction;
- WebSocket correction protection;
- invalid and unconfigured rejection;
- rollback;
- restart persistence;
- BTC/ETH stream isolation.
