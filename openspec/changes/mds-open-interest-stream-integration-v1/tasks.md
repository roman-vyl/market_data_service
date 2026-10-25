## 1. Publication check
- [ ] 1.1 Publication check (application layer): flagged streams, continuation from `L` per D2/D3 through the existing
      `ImportOpenInterestWindow` (continuous run only, at most `MDS_OI_PUBLISH_WINDOWS_PER_CHECK` windows), yes/no + reason;
      unflagged -> yes without I/O
- [ ] 1.2 Wiring: dedicated Bybit REST client instance (1 attempt, `MDS_OI_PUBLISH_TIMEOUT_SECONDS`), settings
      `MDS_OI_PUBLISH_WINDOWS_PER_CHECK`, `MDS_OI_PUBLISH_TIMEOUT_SECONDS`
- [ ] 1.3 `CommittedBarNotificationWorker`: optional check in `asyncio.to_thread` before `send`; no answer -> warning log, no send
- [ ] 1.4 Tests: unflagged stream identical; OI stored -> no REST, sent; OI missing -> fetched, sent; REST failure -> held, next bar
      fetches both points and is sent; partial answer -> only the continuous run stored, held; restored episode T1..T10 ->
      range filled, one send for T10; L far behind -> W windows per check; no prepared history -> held, no REST;
      WebSocket keeps ingesting while a check waits

## 2. History track without the timer
- [ ] 2.1 `OpenInterestTrackRunner`: no reschedule after a completed pass; remove `oi_track_refresh_seconds` and
      `MDS_OI_TRACK_REFRESH_SECONDS`
- [ ] 2.2 Tests: completed stream is not reconciled again; incomplete stream keeps its existing backoff

## 3. Verification
- [ ] 3.1 `make verify`, `openspec validate --strict`
- [ ] 3.2 Smoke on a copy of the production database on the owner's Mac (SOL flagged, BTC/ETH off): live bars notified only with
      OI, Runtime webhook unchanged, no history re-scan after the first pass
