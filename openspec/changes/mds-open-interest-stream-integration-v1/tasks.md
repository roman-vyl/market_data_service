## 0. Owner decisions before apply
- [ ] 0.1 D3 readiness of a flagged stream: A (OI history complete) or B (unchanged)
- [ ] 0.2 D5 read API: A (opt-in `include_open_interest`) or B (always)
- [ ] 0.3 D6 hashes: A (separate `open_interest_hash`) or B (fold into `market_data_hash`)

## 1. Publication
- [ ] 1.1 `READY_TO_PUBLISH` predicate (storage read: candle committed and, for flagged streams, OI at the same `open_time_ms`)
- [ ] 1.2 `OpenInterestCompleter`: bounded queue, one task, REST off the loop, no operation gate, capped retry schedule, existing importer
- [ ] 1.3 `on_outcome` and `_notify_restored` route flagged streams through the completer; unflagged path unchanged
- [ ] 1.4 Startup tail: admitted flagged stream with latest candle lacking OI -> fill, one notification
- [ ] 1.5 Tests: OI already present -> immediate notification; OI late -> held, then one notification; OI never -> no notification,
      retries logged; WebSocket keeps ingesting while held; restored episode -> OI filled, one notification; restart tail -> one
      notification; unflagged stream -> identical notifications to today

## 2. Readiness (if D3 = A)
- [ ] 2.1 OI runner reports `open_interest_history` through the runtime status store for flagged streams
- [ ] 2.2 `/readiness` diagnostic `open_interest_pending_open_time_ms`
- [ ] 2.3 Tests: flagged stream not ready until OI audit gaps = 0; pending live OI does not revoke readiness

## 3. Read API
- [ ] 3.1 Joint window for flagged streams on `/v1/candles` and `/v1/historical-candles`
- [ ] 3.2 `include_open_interest` (D5) and `open_interest_hash` / `expected_open_interest_hash` (D6); OpenAPI updated
- [ ] 3.3 Tests: unflagged and non-opt-in responses byte-identical to today, including `market_data_hash`; OI hole inside range rejected;
      range before OI lower bound rejected; stored SOL hashes still replay

## 4. Verification
- [ ] 4.1 `make verify`
- [ ] 4.2 Smoke on a copy of the production database on the owner's Mac (SOL flagged, BTC/ETH off): live bars notified only with
      OI, Runtime webhook unchanged, `/v1/candles` with and without opt-in
