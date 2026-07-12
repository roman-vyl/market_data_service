# Tasks: Runtime Startup Orchestration v1

- [ ] Define environment settings and precedence.
- [ ] Add validated configured-stream runtime loading.
- [ ] Add a top-level startup coordinator that composes existing use cases.
- [ ] Implement interrupted-state recovery before new startup work.
- [ ] Enforce a finite configurable startup REST-window budget.
- [ ] Add per-stream startup outcomes and aggregate readiness projection.
- [ ] Add `/health` endpoint.
- [ ] Add `/readiness` endpoint with per-stream reasons.
- [ ] Add structured startup/recovery/state-transition logging.
- [ ] Add graceful shutdown and resource cleanup.
- [ ] Add production Dockerfile and persistent-volume compose setup.
- [ ] Add offline startup/restart/failure-isolation tests.
- [ ] Add Docker restart smoke with persisted SQLite.
- [ ] Update README, run instructions, acceptance matrix, and base task statuses.
