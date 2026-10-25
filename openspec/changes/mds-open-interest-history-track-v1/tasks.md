## 0. Bybit checks before code (read-only, owner's Mac)
- [x] 0.1 Window before OI history: empty list or error
- [x] 0.2 200-point `startTime`/`endTime` windows complete without cursor
- [x] 0.3 Timestamp grid and which bar the point at T describes
- [x] 0.4 Is the current open interval returned

## 1. Configuration
- [x] 1.1 Optional `open_interest` per instrument, requires `5m`; tests for on, off, missing `5m`; README and example `markets.toml`

## 2. Storage
- [x] 2.1 Schema v3: `open_interest` and `track_progress` tables, migration v2 -> v3 adds tables only
- [x] 2.2 Unit of work: read/insert/replace OI, list OI open times, get/save track progress
- [x] 2.3 Tests: migration keeps every existing row; exact decimal round-trip; identity uniqueness; progress write leaves `stream_state` unchanged

## 3. Domain and source
- [x] 3.1 `ObservedOpenInterest`, validation, acceptance (committed / duplicate / corrected / rejected)
- [x] 3.2 Port `OpenInterestSource`, adapter `BybitOpenInterestSource` on the existing transport and retry; parser tests from recorded payloads

## 4. Track-neutral parametrization (candle behavior unchanged)
- [x] 4.1 Lower bound: injected window probe and progress store; candle instance unchanged
- [x] 4.2 Audit: injected open-time reader; candle default unchanged
- [x] 4.3 `ImportOpenInterestWindow`, one transaction per window, quarantine for unexpected rows
- [x] 4.4 Repair: injected importer and progress recorder; OI recorder never touches lifecycle
- [x] 4.5 `OpenInterestTrackReconciler`
- [x] 4.6 All existing candle tests pass without changes to their expectations

## 5. Runtime
- [x] 5.1 Worker hooks (on completion, progress reporting); candle instance keeps current hooks
- [x] 5.2 OI worker task after candle startup, shared operation gate, `MDS_OI_TRACK_WINDOWS_PER_PASS` (2), `MDS_OI_TRACK_REFRESH_SECONDS` (300)
- [x] 5.3 Containment: OI exceptions never end the TaskGroup, never change stream lifecycle, readiness or runtime status
- [x] 5.4 Pass log lines

## 6. Proof
- [x] 6.1 Automatic cycle summary line and stored audit result (no manual command)
- [x] 6.2 Integration smoke `tests/test_open_interest_service_smoke.py`: real `serve` process, flag only, fake Bybit REST + silent WebSocket, restart resumes
- [x] 6.3 Tests: BTC-like case (candles before OI) and TRX-like case (OI before candles); restart mid-history; fatal and recoverable OI failure while a stream is `ready`; realtime recovery waits at most one OI pass

## 7. Verification
- [x] 7.1 Targeted tests, then `make verify` green (357 tests)
- [x] 7.2 `openspec validate --all --strict` on the owner's Mac: 12 passed, 0 failed
