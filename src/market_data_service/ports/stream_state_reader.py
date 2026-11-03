"""Read-only access to committed per-stream lifecycle state."""

from typing import Protocol

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamStateSnapshot


class StreamStateReader(Protocol):
    def get_stream_state(self, stream: StreamKey) -> StreamStateSnapshot: ...
