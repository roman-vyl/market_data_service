# Entrypoints

Owns process composition and wiring only:

- service startup;
- dependency construction;
- HTTP server launch;
- graceful shutdown;
- finite backfill, audit, and repair administrative CLI commands.

Entrypoints call application use cases and must not contain market-data business rules.
