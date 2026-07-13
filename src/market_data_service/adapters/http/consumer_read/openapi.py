"""Maintained OpenAPI document for Consumer Read API v1."""

from __future__ import annotations


def _parameter(name: str, schema: dict[str, object]) -> dict[str, object]:
    return {
        "name": name,
        "in": "query",
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
        "required": [
            "open_time_ms",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
        "properties": {
            "open_time_ms": {"type": "integer"},
            "open": decimal_schema,
            "high": decimal_schema,
            "low": decimal_schema,
            "close": decimal_schema,
            "volume": decimal_schema,
        },
    }
    response_schema = {
        "type": "object",
        "required": ["ticker", "timeframe", "from_ms", "to_ms", "candles"],
        "properties": {
            "ticker": {"type": "string"},
            "timeframe": {"type": "string"},
            "from_ms": {"type": "integer"},
            "to_ms": {"type": "integer"},
            "candles": {"type": "array", "items": candle_schema},
        },
    }
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
                        _parameter(
                            "from_ms",
                            {"type": "integer", "minimum": 0},
                        ),
                        _parameter(
                            "to_ms",
                            {"type": "integer", "minimum": 0},
                        ),
                    ],
                    "responses": {
                        "200": {
                            "description": "Complete ready-stream range",
                            "content": {
                                "application/json": {"schema": response_schema}
                            },
                        },
                        "404": {"description": "Configured stream not found"},
                        "409": {"description": "Stream not ready"},
                        "422": {
                            "description": (
                                "Invalid, unaligned, or out-of-bounds range"
                            )
                        },
                        "500": {
                            "description": (
                                "Ready-stream continuity invariant broken"
                            )
                        },
                    },
                }
            }
        },
    }
