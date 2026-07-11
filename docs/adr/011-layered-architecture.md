# ADR-011: Layered architecture and small modules

**Status:** Accepted

## Context

The service must remain maintainable as storage, REST, WebSocket, and API code are added.

## Decision

Use the dependency direction:

```text
domain <- application <- ports/adapters <- entrypoints
```

Modules stay narrow. Generic manager, helper, or utility dumping grounds and mega-files are prohibited.

## Consequences

- Domain has no SQLite, Bybit, HTTP, or process-lifecycle imports.
- Application coordinates use cases through ports.
- Adapters translate external protocols and persistence.
- Architecture tests enforce dependency and size rules.

## Rejected alternatives

- One service or database class owning unrelated responsibilities.
- Business decisions inside adapters or HTTP handlers.
