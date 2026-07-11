# Adapters

Contains concrete infrastructure integrations only:

- Bybit REST and WebSocket;
- SQLite repositories and migrations;
- HTTP API;
- system clock and logging.

Adapters normalize external data and implement ports. They must not decide canonical acceptance, gap policy, readiness policy, or downstream orchestration.
