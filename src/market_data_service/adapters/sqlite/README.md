# SQLite adapter

Version 1 deliberately uses a small six-table schema:

- `schema_meta`
- `instruments`
- `streams`
- `candles`
- `stream_state`
- `quarantine`

The normative DDL is `schema_v1.sql`.

The adapter must preserve these invariants:

1. one database owner process;
2. `PRAGMA journal_mode=WAL`;
3. `PRAGMA synchronous=NORMAL`;
4. `PRAGMA busy_timeout=30000`;
5. `PRAGMA foreign_keys=ON` for every connection;
6. one candle per `(stream_id, open_time_ms)`;
7. exact duplicates are no-ops;
8. conflicting candles are never silently overwritten;
9. candle write and `stream_state` update share one transaction;
10. one REST response window is the historical transaction boundary.

No event log, bootstrap-run table, correction-history table, or metadata-revision table exists in schema v1.
