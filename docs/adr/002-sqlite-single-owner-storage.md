# ADR-002: SQLite single-owner storage

**Status:** Accepted

## Context

The service has one writer, a modest number of symbols, and several million minute candles per symbol. Operational simplicity is more valuable than distributed database features in v1.

## Decision

Use SQLite for schema v1. The database file is owned exclusively by Market Data Service. Other services must not mount or open it directly.

## Consequences

- Simple deployment and backup.
- WAL supports concurrent reads and writes inside the service.
- Consumers later use the service API.

## Rejected alternatives

- PostgreSQL in v1.
- Shared SQLite access across containers.
