"""SQLite implementation of the focused consumer candle read port."""

from __future__ import annotations

from pathlib import Path

from market_data_service.adapters.sqlite.candle_repository import SqliteCandleRepository
from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.adapters.sqlite.connection import open_connection
from market_data_service.adapters.sqlite.stream_state_repository import SqliteStreamStateRepository
from market_data_service.domain.identity import StreamKey
from market_data_service.ports.consumer_read import ConsumerReadSnapshot


class SqliteConsumerCandleReader:
    def __init__(self, database_path: Path | str) -> None:
        self._database_path = database_path

    def read_snapshot(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> ConsumerReadSnapshot:
        with open_connection(self._database_path) as connection:
            connection.execute("BEGIN")
            catalog = SqliteCatalogRepository(connection)
            state = SqliteStreamStateRepository(connection, catalog).get(stream)
            candles = SqliteCandleRepository(connection, catalog).list_range(
                stream,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            return ConsumerReadSnapshot(state=state, candles=candles)
