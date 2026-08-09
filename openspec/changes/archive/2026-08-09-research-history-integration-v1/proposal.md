# Proposal: Research history integration v1

## Motivation

Research Service history-window planning requires canonical storage facts from Market Data Service without coupling historical backtests to realtime stream readiness.

The existing MDS application layer already owns explicit-range continuity audit semantics, but those semantics were not available over HTTP. The canonical candle-range response also lacked an MDS-owned provenance identity.

## Scope

Add three read-only producer contracts:

1. committed candle bounds for one configured stream;
2. explicit-range continuity audit over HTTP;
3. canonical `market_data_hash` on candle-range responses.

The change does not alter the persisted stream lifecycle, readiness transitions, backfill, repair, or Bybit ingestion behavior.

## Non-goals

- changing `ready`, `degraded`, or other lifecycle meanings;
- automatically invoking repair or backfill from a planning request;
- introducing indicator warmup planning;
- changing the existing readiness admission policy of `/v1/candles`;
- implementing Research Service or Strategy Engine changes in this repository.
