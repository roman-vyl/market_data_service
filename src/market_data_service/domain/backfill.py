"""Pure contracts for bounded sequential historical work."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.identity import StreamKey


class BackfillSelection(StrEnum):
    """How an administrative backfill run selects streams."""

    ONE_STREAM = "one_stream"
    ALL_STREAMS = "all_streams"


@dataclass(frozen=True, slots=True)
class BackfillBudget:
    """Finite amount of REST work allowed in one command invocation."""

    max_windows: int

    def __post_init__(self) -> None:
        if self.max_windows <= 0:
            raise ValueError("max_windows must be positive")


@dataclass(frozen=True, slots=True)
class BackfillRequest:
    """One bounded, sequential administrative backfill request."""

    selection: BackfillSelection
    budget: BackfillBudget
    stream: StreamKey | None = None

    def __post_init__(self) -> None:
        if self.selection is BackfillSelection.ONE_STREAM and self.stream is None:
            raise ValueError("one_stream selection requires stream")
        if self.selection is BackfillSelection.ALL_STREAMS and self.stream is not None:
            raise ValueError("all_streams selection must not pin one stream")
