# Design: Canonical Spec Semantic Cleanup v1

## Rule

- Archived change directories under `openspec/changes/archive/2026-08-09-*` remain
  unmodified historical evidence of what those changes did and how.
- A canonical `openspec/specs/**/spec.md` capability contains only durable,
  currently-true contract and architectural invariants of the running system.
- Implementation sequencing, acceptance/benchmark evidence, delivery
  bookkeeping (cumulative patch contents, documentation-artifact checklists),
  and instructions addressed to a different repository do not belong in a
  canonical capability, even if they were true and necessary during delivery.
- Architectural boundaries (module ownership, forbidden imports, transport
  vs. storage separation) are kept when they are still true and still matter
  for preventing regression — they are not removed merely for not being an
  HTTP-visible contract.
