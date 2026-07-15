"""Maintained OpenAPI document for Market Data Service read contracts."""

from __future__ import annotations


def _parameter(
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


def openapi_document() -> dict[str, object]:
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
    candle_response = {
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
    bounds_response = {
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
    audit_response = {
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
        },
    }
    stream_parameters = [
        _parameter("ticker", {"type": "string"}, location="path"),
        _parameter("timeframe", {"type": "string"}, location="path"),
    ]
    return {
        "openapi": "3.1.0",
        "info": {"title": "Market Data Service", "version": "0.1.0"},
        "paths": {
            "/v1/candles": {
                "get": {
                    "summary": "Read one complete canonical candle range",
                    "parameters": [
                        _parameter("ticker", {"type": "string"}),
                        _parameter("timeframe", {"type": "string"}),
                        _parameter("from_ms", {"type": "integer", "minimum": 0}),
                        _parameter("to_ms", {"type": "integer", "minimum": 0}),
                    ],
                    "responses": {
                        "200": {
                            "description": "Complete ready-stream range",
                            "content": {"application/json": {"schema": candle_response}},
                        },
                        "404": {"description": "Configured stream not found"},
                        "409": {"description": "Stream not ready"},
                        "422": {"description": "Invalid or unavailable range"},
                        "500": {"description": "Continuity invariant broken"},
                    },
                }
            },
            "/v1/streams/{ticker}/{timeframe}/bounds": {
                "get": {
                    "summary": "Read committed storage bounds",
                    "parameters": stream_parameters,
                    "responses": {
                        "200": {
                            "description": "Committed candle bounds",
                            "content": {"application/json": {"schema": bounds_response}},
                        },
                        "404": {"description": "Configured stream not found"},
                    },
                }
            },
            "/v1/streams/{ticker}/{timeframe}/continuity-audits": {
                "post": {
                    "summary": "Audit one explicit historical range",
                    "parameters": stream_parameters,
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["start_time_ms", "end_time_ms"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "start_time_ms": {"type": "integer"},
                                        "end_time_ms": {"type": "integer"},
                                    },
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Continuity report",
                            "content": {"application/json": {"schema": audit_response}},
                        },
                        "404": {"description": "Configured stream not found"},
                        "422": {"description": "Invalid or unaligned range"},
                    },
                }
            },
        },
    }
