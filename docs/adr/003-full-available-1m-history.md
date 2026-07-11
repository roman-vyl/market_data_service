# ADR-003: Full available 1m history

**Status:** Accepted

## Context

The database may serve both live consumers and research. A complete minute history is expensive to bootstrap once but cheap to maintain afterward.

## Decision

For each configured ticker, store the full available Bybit 1m history. `launchTime` is only a search floor; the service must discover the first candle actually available from the kline API.

## Consequences

- Heavy initial bootstrap is expected.
- Bootstrap must be resumable and bounded by command options.
- Higher timeframes may later be derived or materialized.

## Rejected alternatives

- Shallow rolling retention by default.
- Treating `launchTime` as the guaranteed first candle.
