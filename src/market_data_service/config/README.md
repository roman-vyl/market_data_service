# Configuration layer

This package will load and validate deployment configuration such as
`config/markets.toml`.

For local Docker Compose, the host-side `config/markets.toml` is mounted
read-only into the container at `/app/config/markets.toml`. Image rebuild is
therefore not required for ordinary config edits; a container restart is
required for the running process to reconcile the new file.

It may translate serialized configuration into domain-neutral startup commands,
but it must not contain exchange clients, SQL, ingestion logic, or mutable
per-stream runtime state.

Market coverage is explicitly multi-instrument. No hard-coded `BTCUSDT`
default is allowed in production paths.
