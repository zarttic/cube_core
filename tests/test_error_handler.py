import asyncio
import json

from starlette.requests import Request

from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ParseError, ValidationError
from grid_core.app.main import handle_grid_core_error


def _request() -> Request:
    return Request(scope={"type": "http", "method": "GET", "path": "/", "headers": []})


def test_error_handler_returns_501_for_not_implemented():
    response = asyncio.run(handle_grid_core_error(_request(), NotImplementedCapabilityError("not ready")))
    payload = json.loads(response.body)
    assert response.status_code == 501
    assert payload == {"error": {"code": "NOT_IMPLEMENTED_CAPABILITY", "message": "not ready"}}


def test_error_handler_returns_422_for_validation_error():
    response = asyncio.run(handle_grid_core_error(_request(), ValidationError("bad input")))
    payload = json.loads(response.body)
    assert response.status_code == 422
    assert payload == {"error": {"code": "VALIDATION_ERROR", "message": "bad input"}}


def test_error_handler_returns_400_for_parse_error():
    response = asyncio.run(handle_grid_core_error(_request(), ParseError("bad st code")))
    payload = json.loads(response.body)
    assert response.status_code == 400
    assert payload == {"error": {"code": "PARSE_ERROR", "message": "bad st code"}}


def test_error_handler_returns_400_for_generic_grid_core_error():
    response = asyncio.run(handle_grid_core_error(_request(), GridCoreError("generic bad", code="GRID_CORE_ERROR")))
    payload = json.loads(response.body)
    assert response.status_code == 400
    assert payload == {"error": {"code": "GRID_CORE_ERROR", "message": "generic bad"}}
