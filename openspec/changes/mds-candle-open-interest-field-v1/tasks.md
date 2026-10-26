## 0. Baseline

- [ ] 0.1 Branch from main a120539. `make verify` green before changes.

## 1. One snapshot, one query

- [ ] 1.1 `SqliteConsumerCandleReader.read_snapshot`: replace the candle read with one query `candles LEFT JOIN open_interest ON stream_id AND open_time_ms`, ordered by open time, inside the existing `BEGIN` that reads the state. `SqliteCandleRepository.list_range` and its other callers stay unchanged. Verify: tests.
- [ ] 1.2 `ConsumerReadSnapshot` and `CandleRangeResult` carry the OI values next to the candles (same order, `str | None` per candle). `CanonicalCandle` and the candle domain stay unchanged. Verify: mypy, tests.

## 2. Response and hash

- [ ] 2.1 Serializer adds `open_interest` to every candle (decimal string or `null`). Verify: HTTP tests for both routes.
- [ ] 2.2 `canonical_market_data_hash` unchanged. The hash of a range is the same with and without stored OI, and the historical read with an old hash still succeeds after OI is stored. Verify: tests.
- [ ] 2.3 OpenAPI candle schema: `open_interest` required, nullable decimal string. Verify: OpenAPI test.

## 3. Cases

- [ ] 3.1 Flagged 5m stream: a bar with OI gets its own value, and a bar with no OI row gets `null`, even when its neighbours have OI. No fill.
- [ ] 3.2 Instrument without OI, and the 1h stream of a flagged instrument: every `open_interest` is `null`.
- [ ] 3.3 Bars before the OI history start in a `GET /v1/candles` window: `null`.

## 4. Verify

- [ ] 4.1 `make verify`, `openspec validate mds-candle-open-interest-field-v1 --strict`.
