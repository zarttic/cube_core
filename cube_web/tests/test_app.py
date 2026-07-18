from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

from fastapi.testclient import TestClient

from cube_web.app import app

client = TestClient(app)


def _token(secret: str, role: str) -> str:
    def encode(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")

    header = encode({"alg": "HS256", "typ": "JWT"})
    payload = encode({"sub": role, "role": role, "exp": time.time() + 60})
    signature = base64.urlsafe_b64encode(hmac.new(secret.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()).decode().rstrip("=")
    return f"{header}.{payload}.{signature}"


def _routes() -> set[tuple[str, str]]:
    return {
        (route.path, method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }


def test_root_smoke_endpoint() -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"service": "cube-web", "status": "ok"}


def test_health_endpoint_reports_service_status() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] in {"ok", "degraded"}


def test_auth_config_has_no_runtime_mode_switch() -> None:
    response = client.get("/api/config")

    assert response.status_code == 200
    body = response.json()
    assert "m6_mode" not in body
    assert "auth_required" in body


def test_sdk_locate_endpoint_uses_encoder_contract(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")
    response = client.post(
        "/v1/grid/locate",
        json={"grid_type": "geohash", "point": [116.391, 39.907], "requested_grid_level": 6},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["cell"]["grid_type"] == "geohash"
    assert body["cell"]["grid_level"] == 6
    assert body["cell"]["space_code"]


def test_partition_openapi_exposes_only_formal_submission_contract() -> None:
    paths = client.get("/openapi.json").json()["paths"]

    assert "/v1/partition/runs" in paths
    assert "/v1/partition/load-batches" in paths
    assert "/v1/partition/schemas/import" in paths
    assert "/v1/partition/tasks/{task_id}" in paths
    for path in (
        "/v1/partition/batches",
        "/v1/partition/tasks/run",
        "/v1/partition/schemas/reconcile",
        "/v1/partition/optical/demo",
        "/v1/partition/optical/test",
        "/v1/partition/assets/retry",
    ):
        assert path not in paths


def test_formal_domain_routes_are_mounted_without_feature_flag() -> None:
    routes = _routes()

    expected = {
        ("/v1/partition/load-batches", "GET"),
        ("/v1/partition/runs", "POST"),
        ("/v1/datasets", "GET"),
        ("/v1/quality/records", "GET"),
        ("/v1/ingest-runs", "GET"),
    }
    assert expected <= routes


def test_loader_import_requires_normalized_dataset_hierarchy() -> None:
    response = client.post(
        "/v1/partition/schemas/import",
        json={
            "schema_version": "1.0",
            "batch_id": "legacy-flat-batch",
            "data_type": "optical",
            "assets": [],
        },
    )

    assert response.status_code == 422


def test_auth_required_protects_formal_api_and_keeps_loader_import_public(monkeypatch) -> None:
    from cube_web.routes import partition as partition_routes

    secret = "test-secret"
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "1")
    monkeypatch.setenv("CUBE_WEB_AUTH_JWT_SECRET_KEY", secret)

    assert client.get("/v1/partition/tasks").status_code == 401
    assert client.post("/v1/partition/schemas/import", json={}).status_code == 422

    user = {"Authorization": f"Bearer {_token(secret, 'user')}"}
    admin = {"Authorization": f"Bearer {_token(secret, 'admin')}"}
    assert client.post("/v1/datasets/missing/archive", json={"reason": "test"}, headers=user).status_code == 403
    assert client.post("/v1/datasets/missing/archive", json={"reason": "test"}, headers=admin).status_code == 404
    monkeypatch.setattr(partition_routes.partition_workflow_service, "cancel_task", lambda task_id: {"task_id": task_id, "status": "cancelled"})
    assert client.post("/v1/partition/tasks/missing/cancel", headers=user).status_code == 403
    assert client.post("/v1/partition/tasks/missing/cancel", headers=admin).status_code == 200


def test_config_contract_uses_current_grid_types(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")
    response = client.post("/v1/config/get", json={})

    assert response.status_code == 200
    body = response.json()
    serialized = str(body).lower()
    assert "tile_matrix" not in serialized
    assert "plane_grid" not in serialized
