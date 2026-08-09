# Proposal: Canonical Spec Semantic Cleanup v1

## Why

The 2026-08-09 archive pass mechanically converted four legacy change specs into
OpenSpec delta format and archived them. The legacy specs predated a clean
separation between durable product contract and implementation/delivery process,
so several archived requirements are process/delivery/cross-repo history
(pre-implementation gates, acceptance benchmarks, cumulative-patch bookkeeping,
instructions to another repository, one-off verification artifacts) rather than
current MDS runtime contract. Archiving promoted that history into
`openspec/specs/**` as if it were a permanent capability contract.

## What changes

Remove or trim the process/delivery/cross-repo portions of four canonical
capabilities so each contains only durable, currently-true MDS behavior:

- `consumer-read-api-v1`
- `websocket-realtime-recovery-v1`

`validated-market-config-and-multi-stream-backfill-v1` and
`historical-backtest-read-contract-v1` were reviewed and contain no such
material; they are left untouched.

## Non-goals

- No Python, test, Docker/Compose, database, or webhook changes.
- No change to any public endpoint, error mapping, or runtime behavior.
- No change to `market-data-service-v1`, `runtime-startup-orchestration-v1`,
  `runtime-continuous-reconciliation-v1`, `research-history-integration-v1`,
  `hardening-operations-v1`, `bounded-recovery-convergence-v1`, or
  `mds-runtime-committed-bar-webhook-v1`.
- No rewriting of the 2026-08-09 archive directories; they remain historical
  evidence of the changes as originally completed.
