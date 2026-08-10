"""Maintained OpenAPI document for Market Data Service read contracts."""

from __future__ import annotations

from market_data_service.adapters.http.consumer_read.openapi_schemas import (
    audit_request_schema,
    audit_response_schema,
    bounds_response_schema,
    candle_response_schema,
    error_response,
    historical_candle_request_schema,
    parameter,
)


def openapi_document() -> dict[str, object]:
    candle_response = candle_response_schema()
    bounds_response = bounds_response_schema()
    audit_response = audit_response_schema()
    historical_candle_request = historical_candle_request_schema()
    stream_parameters = [
        parameter("ticker", {"type": "string"}, location="path"),
        parameter("timeframe", {"type": "string"}, location="path"),
    ]
    return {
        "openapi": "3.1.0",
        "info": {"title": "Market Data Service", "version": "0.1.0"},
        "paths": {
            "/v1/candles": {
                "get": {
                    "summary": "Read one complete canonical candle range",
                    "parameters": [
                        parameter("ticker", {"type": "string"}),
                        parameter("timeframe", {"type": "string"}),
                        parameter("from_ms", {"type": "integer", "minimum": 0}),
                        parameter("to_ms", {"type": "integer", "minimum": 0}),
                    ],
                    "responses": {
                        "200": {
                            "description": "Complete ready-stream range",
                            "content": {"application/json": {"schema": candle_response}},
                        },
                        "404": error_response("configured_stream_not_found"),
                        "409": error_response("stream_not_ready"),
                        "422": error_response(
                            "invalid_request, range_not_aligned, or range_out_of_bounds"
                        ),
                        "500": error_response("continuity_invariant_broken or internal_error"),
                    },
                }
            },
            "/v1/historical-candles": {
                "post": {
                    "summary": "Read one hash-bound historical candle range, bypassing readiness",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {"schema": historical_candle_request}
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Complete range matching the expected hash",
                            "content": {"application/json": {"schema": candle_response}},
                        },
                        "404": error_response("configured_stream_not_found"),
                        "409": error_response("coverage_stale"),
                        "422": error_response(
                            "invalid_request (including a malformed "
                            "expected_market_data_hash) or range_not_aligned"
                        ),
                        "500": error_response("continuity_invariant_broken or internal_error"),
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
                        "404": error_response("configured_stream_not_found"),
                        "422": error_response("invalid_request (malformed ticker/timeframe)"),
                        "500": error_response("internal_error"),
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
                            "application/json": {"schema": audit_request_schema()}
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Continuity report",
                            "content": {"application/json": {"schema": audit_response}},
                        },
                        "404": error_response("configured_stream_not_found"),
                        "422": error_response("invalid_request or range_not_aligned"),
                        "500": error_response("internal_error"),
                    },
                }
            },
        },
    }
