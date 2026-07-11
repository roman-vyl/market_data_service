# Configuration layer

This package will load and validate deployment configuration such as
`config/markets.toml`.

It may translate serialized configuration into domain-neutral startup commands,
but it must not contain exchange clients, SQL, ingestion logic, or mutable
per-stream runtime state.

Market coverage is explicitly multi-instrument. No hard-coded `BTCUSDT`
default is allowed in production paths.
