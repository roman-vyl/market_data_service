"""Atomic canonical-storage boundary."""

from __future__ import annotations

from typing import Protocol


class CanonicalCommitUnitOfWork(Protocol):
    """Atomic candle/quarantine plus stream-state transaction capability."""

    def __enter__(self) -> CanonicalCommitUnitOfWork: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...
