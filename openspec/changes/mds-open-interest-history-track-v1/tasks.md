## 0. Bybit checks before code (read-only, owner's Mac)
- [ ] 0.1 Window before OI history: empty list or error
- [ ] 0.2 200-point `startTime`/`endTime` windows complete without cursor
- [ ] 0.3 Timestamp grid and which bar the point at T describes
- [ ] 0.4 Is the current open interval returned

## 1. Configuration
- [ ] 1.1 Optional `open_interest` per instrument, requires `5m`; tests for on, off, missing `5m`; README and example `markets.toml`

## 2. Storage
- [ ] 2.1 Schema v3: `open_interest` and `track_progress` tables, migration v2 -> v3 adds tables only
- [ ] 2.2 Unit of work: read/insert/replace OI, list OI open times, get/save track progress
- [ ] 2.3 Tests: migration keeps every existing row; exact decimal round-trip; identity uniqueness; progress write leaves `stream_state` unchanged

## 3. Domain and source
- [ ] 3.1 `ObservedOpenInterest`, validation, acceptance (committed / duplicate / corrected / rejected)
- [ ] 3.2 Port `OpenInterestSource`, adapter `BybitOpenInterestSource` on the existing transport and retry; parser tests from recorded payloads

## 4. Track-neutral parametrization (candle behavior unchanged)
- [ ] 4.1 Lower bound: injected window probe and progress store; candle instance unchanged
- [ ] 4.2 Audit: injected open-time reader; candle default unchanged
- [ ] 4.3 `ImportOpenInterestWindow`, one transaction per window, quarantine for unexpected rows
- [ ] 4.4 Repair: injected importer and progress recorder; OI recorder never touches lifecycle
- [ ] 4.5 `OpenInterestTrackReconciler`
- [ ] 4.6 All existing candle tests pass without changes to their expectations

## 5. Runtime
- [ ] 5.1 Worker hooks (on completion, progress reporting); candle instance keeps current hooks
- [ ] 5.2 OI worker task after candle startup, shared operation gate, `MDS_OI_TRACK_WINDOWS_PER_PASS` (2), `MDS_OI_TRACK_REFRESH_SECONDS` (300)
- [ ] 5.3 Containment: OI exceptions never end the TaskGroup, never change stream lifecycle, readiness or runtime status
- [ ] 5.4 Pass log lines

## 6. Proof
- [ ] 6.1 Read-only `oi_audit` command
- [ ] 6.2 Tests: BTC-like case (candles before OI) and TRX-like case (OI before candles); restart mid-history; fatal and recoverable OI failure while a stream is `ready`; realtime recovery waits at most one OI pass

## 7. Verification
- [ ] 7.1 Targeted tests, then `make verify` green; `openspec validate --all --strict` green
