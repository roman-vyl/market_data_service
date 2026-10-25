## 1. Bounds
- [x] 1.1 `GetStreamBounds`: know the configured open interest streams; for a flagged stream read OI track progress in the same unit of
      work and return the intersection; `null` pair when either track has no bounds or the intersection is empty; unflagged unchanged
- [x] 1.2 Wiring: pass the configured open interest streams to `GetStreamBounds` (`runtime/wiring.py` `stream_bounds`)

## 2. Tests
- [x] 2.1 Unflagged stream (and a non-5m stream of a flagged instrument): bounds identical to candle bounds
- [x] 2.2 Flagged stream: OI lower bound later than first candle -> earliest = OI lower bound; latest OI behind latest candle -> latest =
      latest OI
- [x] 2.3 Flagged stream: OI lower bound unresolved / no OI committed / empty intersection -> both `null`, state unchanged
- [x] 2.4 Bounds read does not change lifecycle or start fetching (existing read-only test extended to a flagged stream)

## 3. Verification
- [x] 3.1 `make verify`, `openspec validate --strict`
