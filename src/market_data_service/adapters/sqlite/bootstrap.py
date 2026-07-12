"""Small helpers for creating and seeding a schema-v1 database."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.adapters.sqlite.connection import open_connection
from market_data_service.adapters.sqlite.schema import create_schema, validate_schema
from market_data_service.domain.identity import StreamKey


def initialize_database(path: Path | str) -> None:
    connection = open_connection(path)
    try:
        if not _schema_exists(connection):
            create_schema(connection)
        validate_schema(connection)
    finally:
        connection.close()


def register_stream(
    path: Path | str,
    stream: StreamKey,
    *,
    exchange_symbol: str,
    now_ms: int,
) -> None:
    connection = open_connection(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        SqliteCatalogRepository(connection).register_stream(
            stream, exchange_symbol=exchange_symbol, now_ms=now_ms
        )
        connection.commit()
    finally:
        connection.close()


def _schema_exists(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'schema_meta'
        """
    ).fetchone()
    return row is not None
