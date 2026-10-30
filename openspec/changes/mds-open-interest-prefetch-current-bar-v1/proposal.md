# MDS open interest prefetch: accept the current bar's point

## Why
The prefetch (`mds-open-interest-prefetch-v1`, deployed 2026-10-30 14:46Z) never stores anything. Measured live on 2026-10-30: Bybit returns the open interest point labelled T 3-16 s after bar T opens, and returns it for the exact request MDS sends (`startTime=endTime=T`, `limit=1`). But `parse_open_interest_rows` keeps only points with `open_time_ms <= last_closed_open_time_ms(observed_at)`, so during bar T the point T is always discarded, the importer sees an empty answer and every attempt ends as "not published yet". After deploy: 0 prefetched, 4 bars closed without the point, and 13-18 useless requests per bar for each of the two flagged streams (BTCUSDT.P, SOLUSDT.P).

## What changes
One shared parser, two admission rules for the point's time:

- `closed_only` (existing, default): the point's bar must be closed. Used by history, recovery, lower-bound probing and the commit-time fetch.
- `include_current`: additionally the one bar that is open at the observed time may be accepted. A point later than that bar is still rejected. Used only by the prefetch.

The prefetch gets its own instance of the existing open interest source with `include_current`, built next to the existing live one in the wiring. Parsing, format checks, window check, de-duplication, sorting, importer, classification and storage stay the same classes and code.

## Unchanged
Importer, storage, supervisor and frontier rules, publication step, notification worker, gate, recovery, candle ingestion, settings and defaults of the prefetch, historical and live fetch at commit, Runtime and Engine.

## Out of scope
Disabling or tuning the prefetch, TLS stability towards Bybit, gate granularity, stale grace.
