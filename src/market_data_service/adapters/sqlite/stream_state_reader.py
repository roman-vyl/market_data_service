"""Read committed lifecycle state without reserving SQLite's writer slot."""

from collections.abc import Sequence
from pathlib import Path

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.adapters.sqlite.connection import open_read_only_connection
from market_data_service.adapters.sqlite.stream_state_repository import (
    SqliteStreamStateRepository,
    decode_stream_state,
)
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

    def get_stream_states(self, streams: Sequence[StreamKey]) -> tuple[StreamStateSnapshot, ...]:
        requested = tuple(streams)
        if not requested:
            return ()
        connection = open_read_only_connection(self._database_path)
        try:
            connection.execute("BEGIN")
            placeholders = ",".join("(?,?,?)" for _ in requested)
            parameters = tuple(
                value
                for ordinal, stream in enumerate(requested)
                for value in (ordinal, stream.instrument.ticker, stream.timeframe)
            )
            rows = connection.execute(
                f"""WITH requested(ordinal, ticker, timeframe) AS (VALUES {placeholders})
                SELECT requested.ordinal, i.ticker, s.timeframe, st.*
                FROM requested
                LEFT JOIN instruments i ON i.ticker = requested.ticker
                LEFT JOIN streams s ON s.instrument_id = i.id
                    AND s.timeframe = requested.timeframe
                LEFT JOIN stream_state st ON st.stream_id = s.id
                ORDER BY requested.ordinal""",
                parameters,
            ).fetchall()
            result = []
            for stream, row in zip(requested, rows, strict=True):
                if row["stream_id"] is None:
                    raise KeyError(stream.canonical_id)
                if (row["ticker"], row["timeframe"]) != (
                    stream.instrument.ticker,
                    stream.timeframe,
                ):
                    raise ValueError("batch state identity mismatch")
                result.append(decode_stream_state(stream, row))
            return tuple(result)
        finally:
            connection.close()
