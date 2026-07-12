# Proposal: Consumer Read API v1

## Why

Canonical candles and readiness exist internally, but BBB and future consumers need a stable process boundary for ordered range catch-up, latest-candle reads, and readiness gating. Consumers must not read SQLite directly or depend on internal repositories.

## What changes

- add versioned HTTP candle range and latest-candle endpoints;
- add deterministic pagination and exact decimal JSON strings;
- expose per-stream and aggregate readiness through the public contract;
- publish OpenAPI;
- define the readiness-first consumer cursor/catch-up protocol;
- provide an explicit BBB integration contract without adding strategy logic to this service.

## What does not change

- no server-owned consumer cursor;
- no event stream or replay broker;
- no strategy features, signals, or orders;
- no direct BBB database access;
- no push-notification guarantee.
