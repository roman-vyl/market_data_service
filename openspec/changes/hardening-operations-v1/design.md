# Design: Hardening and Operations v1

Hardening tests exercise existing public contracts rather than bypassing layers. Metrics report process health, stream readiness/state, latest committed candle age, REST/WS failures, repair outcomes, quarantine counts, and SQLite operation health. Metrics do not become a second state store.

Database maintenance remains compatible with one service-owner process and persistent-volume SQLite. Backup, integrity checks, WAL checkpointing, and recovery procedures are explicit operator actions.
