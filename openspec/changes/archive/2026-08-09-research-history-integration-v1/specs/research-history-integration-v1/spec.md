# Research history integration v1 specification

## Requirement: committed bounds

MDS SHALL expose committed candle bounds for every configured stream without requiring `ready` state.

- Bounds SHALL be derived from canonical committed candle rows.
- Both bounds SHALL be nullable together for an empty stream.
- Lifecycle state SHALL be informational.
- The query SHALL have no write, audit, repair, backfill, or upstream-fetch side effects.

## Requirement: explicit continuity audit

MDS SHALL expose the existing explicit-range continuity audit over HTTP.

- The request interval SHALL be half-open and timeframe aligned.
- The response SHALL echo exact checked boundaries.
- Missing candles SHALL be returned as exact half-open gaps.
- The operation SHALL not change lifecycle state.
- The operation SHALL not invoke repair or backfill.

## Requirement: canonical provenance

Every successful canonical candle-range response SHALL include an MDS-owned `market_data_hash`.

- The hash SHALL identify ticker, timeframe, exact range and ordered canonical candles.
- Decimal values SHALL use canonical decimal text.
- The result SHALL be deterministic across identical reads.
- A candle or range identity change SHALL change the hash.

## Requirement: compatibility

The existing `/v1/candles` readiness admission semantics SHALL remain unchanged in this change.
