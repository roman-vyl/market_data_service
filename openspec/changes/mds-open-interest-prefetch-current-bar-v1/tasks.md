## 1. Implementation
- [ ] 1.1 `parse_open_interest_rows(..., include_current=False)` and `BybitOpenInterestSource.include_current`; newest admissible time per D1
- [ ] 1.2 `RuntimeWiring.open_interest_prefetch_live` (D2) and use it in `open_interest_composition.open_interest_prefetch`; `open_interest_live` unchanged

## 2. Tests
- [ ] 2.1 Parser, closed-only: the open bar's point is dropped (as today)
- [ ] 2.2 Parser, include_current: the open bar's point is kept; a point of the next bar is dropped; closed points still kept; duplicates and order unchanged
- [ ] 2.3 Source: default instance and `include_current` instance parse the same Bybit answer differently; request parameters identical
- [ ] 2.4 Wiring: publication, recovery, lower-bound and history sources are closed-only; only the prefetch uses include_current
- [ ] 2.5 Real SQLite full path: prefetch stores T during bar T; commit publication sends no request

## 3. Verification
- [ ] 3.1 `ruff check .`, `mypy`, `pytest`, `openspec validate --all --strict`
- [ ] 3.2 After deploy: one day of logs against the 2026-10-30 baseline (design, Acceptance)
