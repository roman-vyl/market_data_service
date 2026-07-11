# ADR-007: Minimal schema v1

**Status:** Accepted

## Context

The service needs durable candles, stream state, and simple diagnostics, but it is not an exchange or a distributed event platform.

## Decision

Schema v1 contains only:

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

## Consequences

- Small and understandable storage model.
- Duplicate and correction handling remain explicit.
- Additional history tables can be added later without changing candle identity.

## Rejected alternatives

- Mandatory event log.
- Bootstrap run/window tables.
- Candle revision history in v1.
- Metadata revision history in v1.
