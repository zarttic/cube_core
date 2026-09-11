from __future__ import annotations

import json
import logging

from fastapi.testclient import TestClient

from cube_web.app import app
from cube_web.routes import client_errors as client_errors_route

client = TestClient(app)


def _payload(**overrides) -> dict:
    event = {
        "ts": "2026-09-11T12:00:00.000Z",
        "level": "error",
        "scope": "PartitionView",
        "message": "任务列表加载失败",
        "route": "/partition",
        "request_id": "req-42",
        "status": 500,
        "code": "internal_error",
    }
    event.update(overrides)
    return {"client": {"version": "1.0.0", "ua": "pytest"}, "events": [event]}


def test_client_errors_are_accepted_without_token(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    response = client.post("/v1/client-errors", json=_payload())

    assert response.status_code == 202
    assert response.json() == {"status": "accepted", "count": 1}


def test_client_errors_are_logged_with_correlation_fields(monkeypatch, caplog) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")

    with caplog.at_level(logging.WARNING, logger="cube_web.client_errors"):
        response = client.post(
            "/v1/client-errors",
            json=_payload(),
            headers={"X-Request-ID": "req-access-1"},
        )

    assert response.status_code == 202
    records = [record for record in caplog.records if record.name == "cube_web.client_errors"]
    assert len(records) == 1
    message = records[0].getMessage()
    assert "client.error" in message
    assert "scope=PartitionView" in message
    assert "request_id=req-42" in message
    assert "status=500" in message
    assert "actor=anonymous" in message


def test_client_errors_redact_bearer_tokens(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="cube_web.client_errors"):
        response = client.post(
            "/v1/client-errors",
            json=_payload(message="Authorization: Bearer super-secret-token"),
        )

    assert response.status_code == 202
    assert "super-secret-token" not in caplog.text
    assert "[redacted]" in caplog.text


def test_client_errors_ignore_unknown_fields() -> None:
    response = client.post("/v1/client-errors", json=_payload(unexpected="value", stack="x" * 10))

    assert response.status_code == 202


def test_client_errors_reject_too_many_events() -> None:
    payload = {"events": [{"message": f"failure {index}"} for index in range(11)]}

    response = client.post("/v1/client-errors", json=payload)

    assert response.status_code == 422


def test_client_errors_reject_empty_events() -> None:
    response = client.post("/v1/client-errors", json={"events": []})

    assert response.status_code == 422


def test_client_errors_reject_oversized_body() -> None:
    body = json.dumps({"events": [{"message": "x" * 40000}]})

    response = client.post(
        "/v1/client-errors",
        content=body.encode(),
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 413


def test_client_errors_are_rate_limited(monkeypatch) -> None:
    monkeypatch.setattr(client_errors_route, "limiter", client_errors_route._SlidingWindowLimiter(limit=1))

    first = client.post("/v1/client-errors", json=_payload())
    second = client.post("/v1/client-errors", json=_payload())

    assert first.status_code == 202
    assert second.status_code == 429
    assert second.headers["retry-after"] == "60"
