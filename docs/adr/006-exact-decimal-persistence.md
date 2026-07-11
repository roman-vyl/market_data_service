# ADR-006: Exact Decimal persistence

**Status:** Accepted

## Context

Binary floating-point can create false differences between numerically equal exchange values.

## Decision

Use `Decimal` in the domain, normalized decimal text in SQLite, and decimal strings in JSON APIs. Research adapters explicitly convert to numeric DataFrame types.

## Consequences

- Exact round trips and comparisons.
- `104.5000` and `104.5` are duplicates.
- SQL price arithmetic is not a primary storage responsibility.

## Rejected alternatives

- SQLite `REAL` as canonical storage.
- Raw, non-normalized decimal strings.
- Scaled integers tied to exchange precision metadata.
