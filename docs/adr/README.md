# Architecture Decision Records

This directory contains short, accepted architecture decisions for Market Data Service.

Each ADR records one durable decision, its context, consequences, and rejected alternatives. ADRs do not replace the master plan, OpenSpec, or implementation specifications.

## Accepted decisions

- [ADR-001: Standalone service repository](001-standalone-service-repository.md)
- [ADR-002: SQLite single-owner storage](002-sqlite-single-owner-storage.md)
- [ADR-003: Full available 1m history](003-full-available-1m-history.md)
- [ADR-004: Canonical ticker and Bybit symbol mapping](004-canonical-ticker-mapping.md)
- [ADR-005: One canonical ingestion path](005-one-canonical-ingestion-path.md)
- [ADR-006: Exact Decimal persistence](006-exact-decimal-persistence.md)
- [ADR-007: Minimal schema v1](007-minimal-schema-v1.md)
- [ADR-008: Readiness-first consumer contract](008-readiness-first-consumers.md)
- [ADR-009: Per-stream persisted state machine](009-per-stream-state-machine.md)
- [ADR-010: Sequential bounded backfill](010-sequential-bounded-backfill.md)
- [ADR-011: Layered architecture and small modules](011-layered-architecture.md)
