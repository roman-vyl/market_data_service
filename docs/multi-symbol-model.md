# Multi-symbol Model

Version 1 begins with two independent Bybit perpetual instruments:

```text
BTCUSDT.P <-> BTCUSDT
ETHUSDT.P <-> ETHUSDT
```

Each configured ticker expands into one or more explicit canonical streams, and every stream owns its own persistent `stream_state` row. `1m` is supported but not mandatory. No global `current_symbol`, shared last-candle timestamp, shared bootstrap cursor, or shared readiness flag is allowed.

A failure in ETH ingestion must not corrupt BTC state. Aggregate service readiness may remain strict, but diagnostics and operational decisions are always available per stream.
