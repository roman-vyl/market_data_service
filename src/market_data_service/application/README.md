# Application layer

Owns use cases and operational orchestration through ports:

- ingest observed candle;
- discover historical lower bounds;
- bootstrap full minute history;
- bounded sequential backfill planning;
- audit continuity;
- repair gaps;
- catch up after startup or reconnect;
- calculate readiness.

This layer may depend on `domain` and `ports`, but never on concrete Bybit, SQLite, WebSocket, or HTTP implementations.
