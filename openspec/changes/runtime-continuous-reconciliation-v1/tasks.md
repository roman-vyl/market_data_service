# Tasks: Runtime Continuous Reconciliation v1

## Slice 1 — One-stream reconciliation cycle

- [x] Characterize current one-shot startup loss of `INCOMPLETE` and recoverable outcomes.
- [x] Add a reusable one-stream reconciliation-cycle coordinator with a fixed target window.
- [x] Route runtime historical proof through existing full-window `RepairStreamGaps`.
- [x] Prove empty database, prefix gap, internal gap, suffix gap, and multiple-gap scenarios.
- [x] Prove a bounded budget limits one pass and repeated passes consume only remaining gaps.

## Slice 2 — Continuous runtime ownership

- [x] Retain incomplete streams after the initial startup pass.
- [x] Add one sequential deterministic round-robin reconciliation worker.
- [x] Add per-stream capped recoverable backoff and failure isolation.
- [x] Stop retries for fatal streams without stopping healthy streams.
- [x] Add graceful shutdown behavior for queued and in-flight bounded passes.
- [x] Prove restart reconstructs unfinished work from canonical SQLite through fresh preflight.

## Slice 3 — Realtime admission

- [x] Subscribe the transport to all configured stream topics.
- [x] Add a per-stream admission gate before canonical realtime ingestion.
- [x] Prevent historically incomplete streams from advancing canonical realtime state.
- [x] Admit a stream immediately after continuous post-audit without restarting the process.
- [x] Reconcile the moving tail through the existing realtime recovery path.
- [x] Serialize continuous historical work and realtime REST recovery through one runtime gate.
- [x] Extend status/readiness with stable historical blocking reasons.

## Slice 4 — Acceptance and operations

- [x] Add fairness and recoverable-failure isolation tests across multiple streams.
- [x] Add a fake historical source + fake realtime admission + temporary SQLite convergence matrix.
- [x] Prove one completed stream operates in realtime while another remains historical.
- [x] Prove aggregate readiness is not exposed before every required stream is ready.
- [ ] Run a Docker empty-volume autonomous convergence smoke.
- [ ] Run a Docker restart smoke without manual administrative backfill.
- [x] Update README, runtime design documentation, master plan, and acceptance status.
- [x] Run `make verify` and architecture guards.
