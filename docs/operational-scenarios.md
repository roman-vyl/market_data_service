# Operational Scenarios

This document defines observable service behavior during startup, recovery, and continuous operation. These scenarios are application contracts, not incidental implementation details.

## Multi-symbol execution rule

Every scenario below runs per enabled `StreamKey`, for example `BTCUSDT.P:1m` and `ETHUSDT.P:1m`. Durable cursors, gaps, subscriptions, and readiness must never be shared between symbols. The implementation may schedule streams sequentially initially, but state and outcomes remain independent.

Aggregate readiness is strict by default: every enabled required stream must be ready. Per-stream state is always exposed.

## 1. Terms

### Configured stream

A unique market stream:

```text
ticker / timeframe
```

Example:

```text
BTCUSDT.P / 1m
BTCUSDT.P / 1d
```

Each enabled instrument declares at least one configured stream. `1m` is supported but not mandatory in every configuration.

### Historical lower bound

The earliest timestamp from which the service is expected to maintain a complete canonical series for a configured stream.

For the default full-history policy, this is the earliest candle for the configured stream timeframe that Bybit actually makes available, not merely the instrument's `launchTime`.

### Latest closed open time

The open timestamp of the most recent candle that should already be fully closed according to the timeframe grid and the service clock.

### Ready stream

A stream is ready only when its required historical interval is known, canonical continuity has been audited, all required repair is complete, and startup or reconnect tail recovery has proven the latest closed boundary. Later confirmed WebSocket closes are realtime-live diagnostics, not a prerequisite for reading proven history.

## 2. Cold start: database file does not exist

### Preconditions

- configuration is valid;
- the database path does not exist;
- one or more streams are configured.

### Required behavior

1. Create the parent data directory when allowed by configuration.
2. Create the SQLite database.
3. Apply all migrations.
4. Verify the resulting schema contract.
5. Create durable ingestion-state records for configured streams.
6. Resolve the instrument launch time and observed earliest available candle for each configured stream.
7. Set the default required-history start to that observed earliest stream candle.
8. Calculate the latest fully closed candle boundary.
9. Backfill the full required interval through Bybit REST in bounded windows.
10. Pass every fetched candle through canonical normalization, validation, classification, and atomic commit.
11. Audit continuity across the required interval.
12. Repair any remaining fetchable gaps.
13. Establish realtime WebSocket subscriptions.
14. Perform a post-connect short REST audit to close the startup race.
15. Mark each stream ready only after all required checks pass.

### Failure behavior

- A schema creation failure terminates startup.
- A stream whose lower bound cannot be resolved remains unready.
- A REST failure keeps the affected stream unready but must not corrupt already committed streams.
- The service may remain healthy while one or more streams are unready.

## 3. Cold start: database exists and schema is valid

### Required behavior

1. Open the database with required pragmas.
2. Apply pending migrations.
3. Validate the schema contract.
4. Load persisted ingestion state.
5. Reconcile configured streams with persisted streams.
6. For each configured stream, determine the required audit interval:
   - historical lower bound;
   - latest fully closed candle;
   - existing minimum and maximum canonical candle times.
7. Audit canonical continuity, including internal gaps.
8. Catch up the trailing interval after the latest committed candle.
9. Repair internal gaps through the canonical ingestion path.
10. Establish WebSocket subscriptions.
11. Perform the post-connect audit.
12. Restore readiness per stream.

The service MUST NOT assume that a populated database is continuous merely because it has a recent maximum candle timestamp.

## 4. Cold start: database exists but is empty

An existing empty database is semantically equivalent to a new database after schema validation.

The service performs full lower-bound resolution and backfill for every configured stream.

## 5. Cold start: database exists but schema is invalid

The service MUST NOT silently recreate or overwrite an unknown database.

Required behavior:

- report a schema mismatch;
- remain unready or terminate according to migration policy;
- preserve the file for diagnosis;
- require an explicit supported migration or operator action.

## 6. Historical lower-bound resolution

The old BBB Data Engine used Bybit instruments-info `launchTime` and cached it in metadata. The new service preserves the useful idea but tightens the semantics.

### Important distinction

`launchTime` describes instrument availability metadata. It does not prove that:

- a candle exists exactly at that millisecond;
- the timestamp is aligned to every requested timeframe;
- Bybit kline history returns data from that exact boundary;
- all historical intervals are equally available;
- the first returned candle cannot be later.

### Proposed v1 resolution algorithm

For each venue/category/symbol:

1. Fetch and cache instrument `launchTime` with retrieval metadata.
2. Align it upward to the requested timeframe grid to create a search floor.
3. Query the earliest bounded REST window beginning at that floor.
4. If no candle is returned, advance using bounded search windows until data appears or the latest closed boundary is reached.
5. Treat the first valid returned candle open time as the observed earliest available candle for that stream.
6. Persist both:
   - instrument launch time;
   - observed earliest available candle open time per timeframe.
7. Use the observed earliest candle as the normal continuity-audit lower bound.

### Why persist both values

- launch time is useful exchange metadata;
- earliest available candle is the actual storage/audit contract;
- differences are diagnostically important;
- availability can differ by timeframe or exchange behavior.

### Revalidation policy

The earliest-available result should be stable and cached. It may be explicitly re-resolved when:

- a new stream is configured;
- an operator requests a deep audit;
- Bybit returns evidence of older data;
- a migration changes lower-bound semantics.

Normal restart must not repeatedly scan from launch time once the lower bound is durably established.

## 7. REST window splitting

Every requested interval is half-open:

```text
[start_open_time_ms, end_open_time_ms)
```

The service splits it into deterministic timeframe-aligned windows respecting the current Bybit request limit.

Window splitting MUST be pure and independently tested.

Fetched rows outside the requested stream or window are rejected/quarantined as unexpected; they are not silently accepted.

## 8. Existing internal gaps

### Detection

The service audits expected timestamps on the canonical timeframe grid between the persisted lower bound and latest closed boundary.

### Repair

1. Coalesce consecutive missing timestamps into gap intervals.
2. Split each gap into bounded REST windows.
3. Fetch each window.
4. Process rows through canonical ingestion.
5. Re-run the audit.
6. Persist gap diagnostics.
7. Keep readiness false if fetchable gaps remain.

### Legitimately unavailable history

If Bybit cannot provide candles before the observed earliest available candle, that interval is outside the canonical obligation and is not an active gap.

The system must not repeatedly attempt an impossible pre-history repair on every restart.

## 9. Warm continuous operation

For each confirmed WebSocket candle close:

1. Normalize the payload.
2. Validate the configured stream and close status.
3. Classify the observation.
4. Atomically commit the candle mutation and stream state when new or authoritatively corrected.
5. Leave exact duplicates as no-ops.
6. Update realtime freshness and persisted stream state.

Partial current-candle updates do not become canonical candles in v1.

## 10. WebSocket disconnect and reconnect

1. Mark affected streams degraded.
2. Preserve historical read availability.
3. Reconnect with bounded backoff.
4. Calculate the missing closed interval from durable stream state.
5. Fetch and repair through REST.
6. Audit continuity.
7. Resume live handling.
8. Mark ready after repair and tail continuity checks pass.
9. Advance realtime-live diagnostics after a later confirmed close.

## 11. Process crash after candle commit

If the atomic transaction committed before the crash:

- candle and stream state are both present after restart;
- replay from REST or WebSocket is classified as duplicate;
- the existing canonical row is not mutated.

If the transaction did not commit:

- neither candle nor state advancement is visible;
- restart recovery fetches and commits the candle normally.

## 12. Configured stream added

1. Persist or register the new configured stream.
2. Resolve its historical lower bound.
3. Backfill and audit it independently.
4. Subscribe to realtime data.
5. Mark only that stream unready during bootstrap; existing healthy streams remain available.

## 13. Configured stream removed

Removing a stream stops future ingestion and readiness obligations for that stream.

Historical data is not deleted automatically. Deletion/retention requires a separate explicit operation.

## 14. Readiness summary

A stream is not ready when any required condition is true:

- schema/storage unavailable;
- lower bound unresolved;
- bootstrap incomplete;
- internal or trailing gaps remain;
- repair is running or failed;
- latest closed candle is missing beyond tolerance;
- WebSocket state is stale after realtime mode begins;
- clock state prevents a trustworthy closed-boundary calculation.


## 15. Full minute-history bootstrap

For each configured symbol, the default bootstrap target is the complete available `1m` history.

Required behavior:

1. Resolve and cache instrument launch metadata.
2. Find the earliest actually available aligned minute candle.
3. Persist that candle time as the canonical lower bound.
4. Split the full interval into bounded Bybit REST request windows.
5. Process windows in deterministic chronological order.
6. Persist progress as the latest successfully committed candle so a crash resumes from durable state rather than restarting the entire download.
7. Audit the completed range for internal gaps; backfill progress alone does not prove continuity.
8. Repair gaps before the stream becomes ready.
9. Continue appending closed minute candles after bootstrap.

A long first bootstrap is acceptable. Repeating it after every restart is not.

### Bootstrap delivery rule

Historical persistence and realtime notification are separate concerns.

A future consumer such as BBB research bootstraps bulk history through candle range reads. A live consumer records a bootstrap watermark and then consumes incremental events after that watermark.

The implementation must not force a downstream consumer to replay millions of per-minute realtime-style events merely to obtain an existing historical dataset.

## 16. Higher timeframe policy remains explicit

The existence of canonical `1m` data does not silently decide whether `5m`, `15m`, `1h`, `4h`, and `1d` are:

- fetched natively from Bybit;
- derived from canonical minute candles;
- or stored in both forms with provenance.

That decision requires parity tests against the old BBB research datasets. Until approved, `1m` is the only mandatory canonical timeframe.


## Persisted lifecycle reference

All scenarios map to the states and legal transitions in `docs/stream-state-machine.md`. A persisted `ready` state is never restored on trust alone after restart; continuity and freshness are proven again.

## Operator-bounded deep bootstrap

Deep full-history loading is performed by finite administrative commands. A run may target one ticker or all enabled streams and must stop after its explicit REST-window budget. One REST window commits atomically. An interrupted or completed run never discards committed candles, and a later run resumes after the latest committed candle. Normal daemon startup performs only bounded catch-up and does not launch an unlimited multi-year bootstrap.
