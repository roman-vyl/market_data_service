# Instrument and Stream Semantics

## Scope

Version 1 ingests Bybit USDT perpetual futures. The Bybit source category is configured once as `linear` and is not repeated in every domain key or database row.

## Instrument identity

The stable service identity is the canonical ticker:

```text
BTCUSDT.P
ETHUSDT.P
```

`InstrumentKey` therefore contains only `ticker`.

The exact symbol required by Bybit is stored separately as current metadata/configuration:

```text
BTCUSDT.P <-> BTCUSDT
ETHUSDT.P <-> ETHUSDT
```

The exchange symbol does not redefine instrument identity. The mapping is explicit and unique.

## Stream identity

```text
StreamKey = InstrumentKey + timeframe
```

Examples:

```text
BTCUSDT.P:1m
ETHUSDT.P:1m
BTCUSDT.P:1d
```

Every enabled instrument declares a non-empty set of supported canonical timeframes. `1m` is supported and may be part of production coverage, but it is not mandatory in every configuration. Stream state, gap audit, REST repair, WebSocket subscription, and readiness are isolated by `StreamKey`.

## Deliberately excluded from identity

The v1 domain key does not contain:

- `venue`;
- `category`;
- the word `perpetual`;
- base/quote/settle assets;
- launch time;
- enabled status;
- history policy.

Those are source-level assumptions, current metadata, or operator policy rather than the compact identity used throughout the service.
