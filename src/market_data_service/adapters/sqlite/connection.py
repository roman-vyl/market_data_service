"""SQLite connection creation and required pragmas."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def open_connection(path: Path | str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def open_read_only_connection(path: Path | str) -> sqlite3.Connection:
    """Open existing storage without permitting database mutations or creation."""
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
    except BaseException:
        connection.close()
        raise
    return connection
