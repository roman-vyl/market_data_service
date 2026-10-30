"""Helpers for unwrapping task-group failures."""

from __future__ import annotations


def root_exception(exc: BaseException) -> BaseException:
    """First leaf of nested exception groups."""

    if isinstance(exc, BaseExceptionGroup) and exc.exceptions:
        return root_exception(exc.exceptions[0])
    return exc
