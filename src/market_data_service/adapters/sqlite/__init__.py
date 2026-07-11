"""SQLite persistence adapter."""

from market_data_service.adapters.sqlite.bootstrap import initialize_database, register_stream
from market_data_service.adapters.sqlite.schema import UnsupportedSchemaVersion, validate_schema
from market_data_service.adapters.sqlite.transaction import SqliteUnitOfWork

__all__ = [
    "SqliteUnitOfWork",
    "UnsupportedSchemaVersion",
    "initialize_database",
    "register_stream",
    "validate_schema",
]
