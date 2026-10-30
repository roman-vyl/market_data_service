## D1. Policy lives in the source, not in a global flag
`BybitOpenInterestSource` gets one field `include_current: bool = False`. `parse_open_interest_rows` gets the same keyword, default `False`, and computes the newest admissible open time: `last_closed` for `closed_only`, `last_closed + step` for `include_current` (the bar open at `observed_at_ms`). Points above it are dropped exactly as now. No other layer (importer, port, supervisor, publication) learns about the policy.

## D2. Only the prefetch uses it
`RuntimeWiring.open_interest_live(timeout)` keeps building the closed-only source for the publication step. A second factory, `open_interest_prefetch_live(timeout)`, builds the same chain (one attempt, short timeout, `ImportOpenInterestWindow`, `AppendLiveOpenInterest`) with `include_current=True`; `open_interest_composition.open_interest_prefetch` uses it. The two instances share the REST client class and all code; they differ only by the field.

## D3. Safety
- A future point (label later than the open bar by the MDS clock) is rejected even in `include_current`.
- The requested window is still checked first, so a point outside it is never stored.
- The prefetch asks for exactly `[T, T+step)` of the open bar, so only that point can be accepted by it.
- Bybit's snapshot T does not change after publication (measured 2026-10-25). If it ever did, the importer classifies `CORRECTED` as before.
- Historical, recovery and commit-time paths cannot reach the new policy: it is a constructor argument of one separate instance.

## Acceptance (after deploy, a day of logs)
Prefetch stores the point 20-35 s after bar open ("open interest prefetched" lines for BTC and SOL on most bars); bars closed without the point and "bar not published" lines for the two streams drop clearly below the 2026-10-30 baseline (59 per 26 h); no request is made at commit when the point is stored; no growth of WebSocket latency or event loop lag; ERROR 0.
