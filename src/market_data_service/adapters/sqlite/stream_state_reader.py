"""Read committed lifecycle state without reserving SQLite's writer slot."""

from pathlib import Path

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.adapters.sqlite.connection import open_read_only_connection
from market_data_service.adapters.sqlite.stream_state_repository import SqliteStreamStateRepository
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamStateSnapshot


class SqliteStreamStateReader:
    def __init__(self, database_path: Path | str) -> None:
        self._database_path = database_path

    def get_stream_state(self, stream: StreamKey) -> StreamStateSnapshot:
        connection = open_read_only_connection(self._database_path)
        try:
            connection.execute("BEGIN")
            catalog = SqliteCatalogRepository(connection)
            return SqliteStreamStateRepository(connection, catalog).get(stream)
        finally:
            connection.close()
