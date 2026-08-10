"""Reusable JSON Schema fragments for the maintained OpenAPI document."""

from __future__ import annotations


def parameter(
    name: str,
    schema: dict[str, object],
    *,
    location: str = "query",
) -> dict[str, object]:
    return {
        "name": name,
        "in": location,
        "required": True,
        "schema": schema,
    }


def error_response(description: str) -> dict[str, object]:
    return {
        "description": description,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["error", "detail"],
                    "properties": {
                        "error": {"type": "string"},
                        "detail": {"type": "string"},
                    },
                }
            }
        },
    }


def candle_response_schema() -> dict[str, object]:
    decimal_schema = {
        "type": "string",
        "pattern": r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$",
    }
    candle_schema = {
        "type": "object",
        "required": ["open_time_ms", "open", "high", "low", "close", "volume"],
        "properties": {
            "open_time_ms": {"type": "integer"},
            "open": decimal_schema,
            "high": decimal_schema,
            "low": decimal_schema,
            "close": decimal_schema,
            "volume": decimal_schema,
        },
    }
    return {
        "type": "object",
        "required": [
            "ticker",
            "timeframe",
            "from_ms",
            "to_ms",
            "market_data_hash",
            "candles",
        ],
        "properties": {
            "ticker": {"type": "string"},
            "timeframe": {"type": "string"},
            "from_ms": {"type": "integer"},
            "to_ms": {"type": "integer"},
            "market_data_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "candles": {"type": "array", "items": candle_schema},
        },
    }


def bounds_response_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": [
            "contract_version",
            "ticker",
            "timeframe",
            "state",
            "earliest_committed_open_time_ms",
            "latest_committed_open_time_ms",
        ],
        "properties": {
            "contract_version": {"const": "market_stream_bounds.v1"},
            "ticker": {"type": "string"},
            "timeframe": {"type": "string"},
            "state": {"type": "string"},
            "earliest_committed_open_time_ms": {
                "type": ["integer", "null"],
                "minimum": 0,
            },
            "latest_committed_open_time_ms": {
                "type": ["integer", "null"],
                "minimum": 0,
            },
        },
    }


def audit_response_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": [
            "contract_version",
            "ticker",
            "timeframe",
            "checked_start_ms",
            "checked_end_ms",
            "candle_count",
            "is_continuous",
            "gaps",
            "state",
            "market_data_hash",
        ],
        "properties": {
            "contract_version": {"const": "market_continuity_audit.v1"},
            "ticker": {"type": "string"},
            "timeframe": {"type": "string"},
            "checked_start_ms": {"type": "integer", "minimum": 0},
            "checked_end_ms": {"type": "integer", "minimum": 1},
            "candle_count": {"type": "integer", "minimum": 0},
            "is_continuous": {"type": "boolean"},
            "gaps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["from_ms", "to_ms"],
                    "properties": {
                        "from_ms": {"type": "integer"},
                        "to_ms": {"type": "integer"},
                    },
                },
            },
            "state": {"type": "string"},
            "market_data_hash": {
                "type": ["string", "null"],
                "pattern": "^[0-9a-f]{64}$",
                "description": (
                    "Canonical hash over the checked range when is_continuous is true; "
                    "null when the checked range contains a gap."
                ),
            },
        },
    }


def audit_request_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": ["from_ms", "to_ms"],
        "additionalProperties": False,
        "properties": {
            "from_ms": {"type": "integer"},
            "to_ms": {"type": "integer"},
        },
    }


def historical_candle_request_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": [
            "ticker",
            "timeframe",
            "from_ms",
            "to_ms",
            "expected_market_data_hash",
        ],
        "additionalProperties": False,
        "properties": {
            "ticker": {"type": "string"},
            "timeframe": {"type": "string"},
            "from_ms": {"type": "integer", "minimum": 0},
            "to_ms": {"type": "integer", "minimum": 0},
            "expected_market_data_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
    }
