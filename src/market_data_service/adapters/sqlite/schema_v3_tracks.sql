-- Open interest is a separate required-data track of a stream (schema v3).
-- Values are canonical decimal text normalized by the domain.
CREATE TABLE open_interest (
    stream_id INTEGER NOT NULL,
    open_time_ms INTEGER NOT NULL,
    open_interest_value TEXT NOT NULL,
    source TEXT NOT NULL,
    committed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (stream_id, open_time_ms),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
    CHECK (open_time_ms >= 0),
    CHECK (source IN ('bybit_rest'))
) WITHOUT ROWID;

-- Internal progress facts of non-candle tracks. No lifecycle state here:
-- stream readiness is owned by stream_state only.
CREATE TABLE track_progress (
    stream_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    earliest_available_open_time_ms INTEGER,
    discovery_next_open_time_ms INTEGER,
    latest_committed_open_time_ms INTEGER,
    last_audit_at_ms INTEGER,
    last_audit_gap_count INTEGER,
    last_success_at_ms INTEGER,
    last_error_code TEXT,
    last_error_detail TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (stream_id, track),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE RESTRICT,
    CHECK (track IN ('open_interest')),
    CHECK (
        earliest_available_open_time_ms IS NULL
        OR discovery_next_open_time_ms IS NULL
    )
);
