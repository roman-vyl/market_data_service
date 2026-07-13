# ADR-012: Native configured timeframes

**Status:** Accepted

## Context

Version 1 supports configured streams beyond `1m`, including `5m`, `1h`, `4h`, and `1d`. The service must avoid silently inventing aggregation rules that differ from Bybit or old BBB research behavior.

## Decision

Every configured timeframe is an independent canonical stream fetched natively from Bybit REST and WebSocket intervals. Higher timeframes are not derived from canonical `1m` candles in v1. `1m` remains supported but is not mandatory in every market configuration.

## Consequences

- Continuity, lower bounds, lifecycle state, readiness, and repair are proven per `StreamKey`.
- Candle identity for higher timeframes follows the configured Bybit interval contract.
- Mathematical equivalence with locally aggregated `1m` candles is not a v1 invariant.
- Derived timeframe storage or comparison can be proposed in a later change with explicit provenance.

## Rejected alternatives

- Require every production configuration to include `1m`.
- Derive all higher timeframes from `1m` during v1 ingestion.
- Store both native and derived higher timeframe candles without a provenance model.
