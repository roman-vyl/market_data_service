## Why

Open interest (OI) is already fetched, stored on the 5m bar grid (`open_interest` table), part of READY and publication, and part of
stream bounds. Consumers cannot read it yet: `GET /v1/candles` and `POST /v1/historical-candles` return OHLCV only.

Owner decision (2026-10-25/26): OI is delivered as an ordinary field of the candle, like `volume`, but nullable. There is one request, one
time key and one SQLite snapshot, with no flag and no new route. OI has its own track semantics inside MDS, and the outside world does not
see them. MDS assembles the field with an SQL query.

Deploy order: Strategy Engine `dd1ce62` (accepts the optional `open_interest` field, SE #46) must be running before this change is
deployed. Engine `42d8bad` and earlier reject unknown candle fields.

## What Changes

- Every candle in a successful response of `GET /v1/candles` and `POST /v1/historical-candles` gets `open_interest`:
  - the JSON decimal string stored in `open_interest` for the **same stream and the same `open_time_ms`**;
  - `null` when MDS has no OI value for that bar: an instrument without OI, a bar before the start of OI history, or a stream other
    than the 5m stream of an instrument with `open_interest = true`.
  - No shift to a neighbouring bar, no forward fill, no zero.
- The consumer reader reads the candles and their OI with **one SQL query** (candles `LEFT JOIN open_interest` on stream and open time),
  inside the existing read transaction that already reads the stream state. OHLCV, OI and state therefore come from one SQLite
  snapshot.
- `market_data_hash` is unchanged: same algorithm, OHLCV only. OI does not take part in `coverage_stale`. Hashes of existing research
  runs stay valid.
- The OpenAPI candle schema documents `open_interest` as a required nullable decimal string.

Not changed: request shapes, routes, errors, range/grid/window validation, READY, publication and notifications, stream bounds,
continuity audits, Runtime, Engine. An OI hash is not part of this change.

## Impact

- `adapters/sqlite/consumer_candle_reader.py` (joined query), `ports/consumer_read.py` and `application/consumer_read/models.py`
  (OI values carried next to the candles), `adapters/http/consumer_read/serialization.py`, `adapters/http/consumer_read/openapi.py`
  and its schemas.
- Spec delta: one `MODIFIED` requirement, *Candle responses preserve exact data and provenance*.
