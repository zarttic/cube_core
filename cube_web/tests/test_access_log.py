from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from cube_web.app import request_id_middleware
from cube_web.services.access_log import access_log_middleware
from cube_web.services.api_errors import request_id_from_header


def _build_app() -> FastAPI:
    local = FastAPI()
    local.middleware("http")(request_id_middleware)
    local.middleware("http")(access_log_middleware)

    @local.get("/ping")
    def ping() -> dict:
        return {"ok": True}

    @local.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @local.get("/missing")
    def missing() -> None:
        raise HTTPException(status_code=404, detail="not here")

    @local.get("/boom")
    def boom() -> None:
        raise RuntimeError("kaboom")

    return local


def _make_request(headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    scope = {"type": "http", "method": "GET", "path": "/", "headers": headers or [], "state": {}}
    return Request(scope)


def test_access_log_records_request_with_request_id(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "1")
    client = TestClient(_build_app())

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/ping", headers={"X-Request-ID": "req-1"})

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "req-1"
    assert "web.access method=GET path=/ping status=200" in caplog.text
    assert "request_id=req-1" in caplog.text


def test_access_log_generates_request_id_when_missing(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "1")
    client = TestClient(_build_app())

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/ping")

    request_id = response.headers["x-request-id"]
    assert request_id
    assert f"request_id={request_id}" in caplog.text


def test_access_log_skips_health(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "1")
    client = TestClient(_build_app())

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/health")

    assert response.status_code == 200
    assert "web.access" not in caplog.text


def test_access_log_marks_client_errors_as_warning(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "1")
    client = TestClient(_build_app())

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/missing")

    assert response.status_code == 404
    record = next(record for record in caplog.records if record.name == "cube_web.access")
    assert record.levelno == logging.WARNING
    assert "status=404" in record.getMessage()


def test_access_log_marks_server_errors_as_error(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "1")
    client = TestClient(_build_app(), raise_server_exceptions=False)

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/boom")

    assert response.status_code == 500
    record = next(record for record in caplog.records if record.name == "cube_web.access")
    assert record.levelno == logging.ERROR
    assert "status=500" in record.getMessage()


def test_access_log_can_be_disabled(monkeypatch, caplog):
    monkeypatch.setenv("CUBE_LOG_ACCESS", "0")
    client = TestClient(_build_app())

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        response = client.get("/ping", headers={"X-Request-ID": "req-off"})

    assert response.status_code == 200
    assert "web.access" not in caplog.text


def test_request_id_from_header_is_idempotent():
    request = _make_request()

    first = request_id_from_header(request)
    second = request_id_from_header(request)

    assert first
    assert first == second


def test_request_id_from_header_prefers_incoming_header():
    request = _make_request([(b"x-request-id", b"abc-123")])

    assert request_id_from_header(request) == "abc-123"
    assert request_id_from_header(request) == "abc-123"


def test_request_id_from_header_replaces_invalid_header():
    request = _make_request([(b"x-request-id", b"bad id with spaces")])

    value = request_id_from_header(request)

    assert value != "bad id with spaces"
    assert request_id_from_header(request) == value


def test_auth_denied_is_logged_with_request_id(monkeypatch, caplog):
    from cube_web.app import app

    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")
    client = TestClient(app)

    with caplog.at_level(logging.INFO, logger="cube_web.access"):
        with caplog.at_level(logging.WARNING, logger="cube_web.routes.auth"):
            response = client.get("/v1/partition/tasks", headers={"X-Request-ID": "req-auth-1"})

    assert response.status_code == 401
    assert response.headers["x-request-id"] == "req-auth-1"
    denied = [record for record in caplog.records if record.getMessage().startswith("auth.denied")]
    assert len(denied) == 1
    assert denied[0].levelno == logging.WARNING
    assert "status=401" in denied[0].getMessage()
    assert "request_id=req-auth-1" in caplog.text
