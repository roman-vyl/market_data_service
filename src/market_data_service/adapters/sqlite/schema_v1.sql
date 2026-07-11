PRAGMA foreign_keys = ON;

CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE instruments (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    exchange_symbol TEXT NOT NULL UNIQUE,
    launch_time_ms INTEGER,
    metadata_fetched_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (ticker <> ''),
    CHECK (exchange_symbol <> ''),
    CHECK (launch_time_ms IS NULL OR launch_time_ms >= 0),
    CHECK (metadata_fetched_at_ms IS NULL OR metadata_fetched_at_ms >= 0)
);

CREATE TABLE streams (
    id INTEGER PRIMARY KEY,
    instrument_id INTEGER NOT NULL,
    timeframe TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE RESTRICT,
    UNIQUE (instrument_id, timeframe),
    CHECK (timeframe <> '')
);

-- OHLCV TEXT values must already satisfy docs/decimal-policy.md.
-- SQLite is persistence only; decimal normalization belongs to the domain.
CREATE TABLE candles (
    stream_id INTEGER NOT NULL,
    open_time_ms INTEGER NOT NULL,
    open_value TEXT NOT NULL,
    high_value TEXT NOT NULL,
    low_value TEXT NOT NULL,
    close_value TEXT NOT NULL,
    volume_value TEXT NOT NULL,
    source TEXT NOT NULL,
    committed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (stream_id, open_time_ms),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
    CHECK (open_time_ms >= 0),
    CHECK (source IN ('bybit_rest', 'bybit_websocket'))
) WITHOUT ROWID;

CREATE TABLE stream_state (
    stream_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    earliest_available_open_time_ms INTEGER,
    latest_committed_open_time_ms INTEGER,
    last_audit_at_ms INTEGER,
    last_rest_success_at_ms INTEGER,
    last_ws_message_at_ms INTEGER,
    last_error_code TEXT,
    last_error_detail TEXT,
    state_changed_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
    CHECK (state IN (
        'uninitialized',
        'bootstrapping',
        'auditing',
        'repairing',
        'connecting',
        'ready',
        'degraded',
        'failed'
    ))
);

CREATE TABLE quarantine (
    id INTEGER PRIMARY KEY,
    stream_id INTEGER,
    start_ms INTEGER,
    end_ms INTEGER,
    reason_code TEXT NOT NULL,
    detail TEXT,
    payload_json TEXT,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
    CHECK (start_ms IS NULL OR start_ms >= 0),
    CHECK (end_ms IS NULL OR end_ms >= 0),
    CHECK (start_ms IS NULL OR end_ms IS NULL OR end_ms > start_ms)
);

CREATE INDEX ix_quarantine_stream_created
ON quarantine(stream_id, created_at_ms);
