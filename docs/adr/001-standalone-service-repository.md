# ADR-001: Standalone service repository

**Status:** Accepted

## Context

BBB research already has a historical data path, while Abi Executor is a separate live execution service. Realtime market-data ingestion has an independent lifecycle and should not force a migration of either system.

## Decision

Market Data Service lives in its own repository, owns its own database and Docker image, and has no runtime imports from BBB or Abi Executor.

## Consequences

- Independent deployment and failure boundary.
- BBB research remains unchanged during initial development.
- Integration happens through explicit APIs and contracts.

## Rejected alternatives

- Extending the historical BBB Data Engine in place.
- Embedding market ingestion inside Abi Executor.
