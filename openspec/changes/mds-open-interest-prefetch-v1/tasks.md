## 1. Implementation
- [ ] 1.1 Settings: `MDS_OI_PREFETCH_DELAY_SECONDS` (20, 0 = off), `MDS_OI_PREFETCH_RETRY_SECONDS` (15), `MDS_OI_PREFETCH_CONCURRENCY` (4); validation like `oi_live_timeout_seconds`
- [ ] 1.2 `runtime/open_interest_prefetch.py` (new module, under the 220-line limit): per-stream attempt state, one-second tick, bar selection, due/stop rules (D2, D3), semaphore + `asyncio.to_thread(append)` (D4), log lines (D6)
- [ ] 1.3 Compose it with the existing open interest composition and start it as a task of `RuntimeRealtimeCoordinator` next to the stale worker; no change to `OpenInterestPublication`, notification worker, supervisor or gate
- [ ] 1.4 Verify and, if needed, handle: audits, track progress and read paths with one open interest point beyond the candle frontier; default executor size with the added threads

## 2. Tests (controlled time, scripted source, no network)
- [ ] 2.1 Point available at +20 s: stored once, no second request at commit, publication sends without REST
- [ ] 2.2 Empty/failed answers then success: retries every 15 s, stops after success
- [ ] 2.3 Never available: attempts stop at bar close, one warning, commit-time fallback runs once as today
- [ ] 2.4 Stream in recovery, not admitted, unflagged or non-5m: no attempt
- [ ] 2.5 Ten streams due together: never more than the configured concurrency in flight; none holds the operation gate or touches the WebSocket path
- [ ] 2.6 Prefetch racing the commit-time fallback: duplicate classified, one notification
- [ ] 2.7 Delay 0 disables the task; behaviour identical to before
- [ ] 2.8 Prefetched point does not move the open interest frontier; the publication step does, by one step
- [ ] 2.9 Shutdown: no new attempts, in-flight attempt finishes

## 3. Verification
- [ ] 3.1 `make verify` green; `openspec validate --all --strict`
- [ ] 3.2 After deploy: compare one day of logs against the 2026-10-29/30 baseline (design, Acceptance)
