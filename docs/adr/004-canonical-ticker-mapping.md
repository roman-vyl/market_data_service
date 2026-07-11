# ADR-004: Canonical ticker and Bybit symbol mapping

**Status:** Accepted

## Context

Human-facing and research-facing ticker notation differs from the exact symbol required by the Bybit API.

## Decision

Use canonical tickers such as `BTCUSDT.P` and `ETHUSDT.P`. Store the exact Bybit API symbols `BTCUSDT` and `ETHUSDT` beside them. Bybit source category `linear` is configured once at source level.

## Consequences

- Stable internal ticker names.
- No repeated exchange taxonomy in candle rows.
- The Bybit adapter never guesses symbol conversion.

## Rejected alternatives

- `BTC/USDT.P` ticker notation.
- Using only the exchange symbol everywhere.
- Repeating `linear` and `perpetual` in every record.
