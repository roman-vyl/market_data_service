## 1. Implementation
- [x] 1.1 Settings: `MDS_OI_PREFETCH_DELAY_SECONDS` (20, 0 = off), `MDS_OI_PREFETCH_RETRY_SECONDS` (15), `MDS_OI_PREFETCH_CONCURRENCY` (4); validation like `oi_live_timeout_seconds`
- [x] 1.2 `runtime/open_interest_prefetch.py` (new module, under the 220-line limit): per-stream attempt state, one-second tick, bar selection, due/stop rules (D2, D3), semaphore + re-check of bar, stored state and recovery right before the request + `asyncio.to_thread(append)` (D4), log lines (D6)
- [x] 1.3 Compose it with the existing open interest composition and start it as a task of `RuntimeRealtimeCoordinator` next to the stale worker; no change to `OpenInterestPublication`, notification worker, supervisor or gate
- [x] 1.4 Checked, no change needed: open interest audits are range-bounded `[start, end)` with `end` = open bar, stream bounds take `min(candle latest, open interest latest)`, candle reads LEFT JOIN from candles, handoff sync uses the stored latest point; default executor (min(32, cpu+4)) holds the 4 extra threads

## 2. Tests (controlled time, scripted source, no network)
- [x] 2.1 Point available at +20 s: stored once, no second request at commit, publication sends without REST
- [x] 2.2 Empty/failed answers then success: retries every 15 s, stops after success
- [x] 2.3 Never available: attempts stop at bar close, one warning, commit-time fallback runs once as today
- [x] 2.4 Stream in recovery, without subscription or not admitted: no attempt; unflagged streams are never given to the task (composition), open interest exists for 5m only
- [x] 2.5 Ten streams due together: never more than the configured concurrency in flight; none holds the operation gate or touches the WebSocket path
- [x] 2.5a Attempt waiting for a slot when the bar closes (or the point gets stored, or recovery starts): no request is sent; a request started before the close completes and is stored
- [x] 2.6 Publication step after a prefetch finds the point stored and sends no request (`test_second_publication_step...`); a true simultaneous double fetch is classified `DUPLICATE` by the existing importer (existing tests), no new test
- [x] 2.7 Delay 0 disables the task; behaviour identical to before
- [x] 2.8 The prefetch has no supervisor reference (only read-only `facts`), so it cannot move the frontier; publication still does, by one step (existing publication tests)
- [x] 2.9 Shutdown: no new attempts, in-flight attempt finishes
- [x] 2.10 Real SQLite: no write lock is held while the REST request runs (a second connection starts `BEGIN IMMEDIATE` inside the fetch; fails if the request moves into the transaction)
- [x] 2.11 Real SQLite, full path: prefetched point, confirmed candle, publication makes no REST request, the frontier moves to T, exactly one notification

## 3. Verification
- [x] 3.1 `ruff check .`, `mypy`, `pytest` (393 passed) and `openspec validate --strict` green; `make verify` equivalents run directly
- [ ] 3.2 After deploy: compare one day of logs against the 2026-10-29/30 baseline (design, Acceptance)
