from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from cube_web.app import (
    app,
    handle_http_exception,
    handle_not_found_error,
    handle_unexpected_error,
    handle_validation_error,
    request_id_middleware,
)
from cube_web.services.quality_repository import OutputVersionNotFound

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
    body = response.json()
    assert body["status"] in {"ok", "degraded"}
    assert body["checks"]["partition_queue"] == {"status": "ok", "queued": 0, "max_workers": 4}


def test_auth_config_has_no_runtime_mode_switch() -> None:
    response = client.get("/api/config")

    assert response.status_code == 200
    body = response.json()
    assert "m6_mode" not in body
    assert "auth_required" in body


def test_auth_me_exposes_upstream_effective_permissions(monkeypatch) -> None:
    from cube_web.services import auth_service

    secret = "test-secret"
    monkeypatch.setenv("CUBE_WEB_AUTH_JWT_SECRET_KEY", secret)
    monkeypatch.setenv("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://auth.example")
    monkeypatch.setattr(
        auth_service,
        "_get_json",
        lambda url, token: {
            "username": "op_data",
            "role": "操作员",
            "is_operator": True,
            "permissions": ["data_import:view", "data_import:operate"],
        },
    )

    response = client.get("/api/me", headers={"Authorization": f"Bearer {_token(secret, 'operator')}"})

    assert response.status_code == 200
    assert response.json()["permissions"] == ["data_import:view", "data_import:operate"]
    assert response.json()["is_operator"] is True


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


def test_web_grid_cover_keeps_real_cells_and_returns_continuous_preview_cells(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")
    response = client.post(
        "/v1/grid/cover",
        json={
            "grid_type": "mgrs",
            "requested_grid_level": 0,
            "cover_mode": "intersect",
            "boundary_type": "polygon",
            "bbox": [100.6447907229, 23.2863720005, 104.8299474060, 27.0611663017],
            "crs": "EPSG:4326",
            "preview_mode": "continuous",
            "preview_crs": "EPSG:32648",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["statistics"]["cell_count"] == 40
    assert len(body["cells"]) == 40
    assert len(body["preview_cells"]) == 30
    assert all(cell["metadata"]["preview_only"] is True for cell in body["preview_cells"])


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
        ("/v1/partition/runs/{partition_run_id}/cancel", "POST"),
        ("/v1/partition/tasks/{task_id}/terminate", "POST"),
        ("/v1/datasets", "GET"),
        ("/v1/datasets/{dataset_id}", "DELETE"),
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
    monkeypatch.setattr(partition_routes.partition_workflow_service, "force_cancel_task", lambda task_id: {"task_id": task_id, "status": "cancelled"})
    assert client.post("/v1/partition/tasks/missing/cancel", headers=user).status_code == 403
    assert client.post("/v1/partition/tasks/missing/cancel", headers=admin).status_code == 200
    assert client.post("/v1/partition/tasks/missing/terminate", headers=user).status_code == 403
    assert client.post("/v1/partition/tasks/missing/terminate", headers=admin).status_code == 200
    assert client.delete("/v1/datasets/missing", headers=user).status_code == 403


def test_config_contract_uses_current_grid_types(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_AUTH_REQUIRED", "0")
    response = client.post("/v1/config/get", json={})

    assert response.status_code == 200
    body = response.json()
    serialized = str(body).lower()
    assert "tile_matrix" not in serialized
    assert "plane_grid" not in serialized


def test_unexpected_api_error_returns_safe_payload_and_request_id() -> None:
    local = FastAPI()
    local.add_exception_handler(Exception, handle_unexpected_error)
    local.middleware("http")(request_id_middleware)

    @local.get("/boom")
    def boom() -> None:
        raise RuntimeError("database password should not be returned")

    response = TestClient(local, raise_server_exceptions=False).get(
        "/boom",
        headers={"X-Request-ID": "request-123"},
    )

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "request-123"
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "服务器内部错误，请稍后重试（请求 ID：request-123）",
            "request_id": "request-123",
        }
    }
    assert "database password" not in response.text


def test_explicit_server_http_exception_does_not_return_backend_detail() -> None:
    local = FastAPI()
    local.add_exception_handler(HTTPException, handle_http_exception)
    local.middleware("http")(request_id_middleware)

    @local.get("/upstream")
    def upstream() -> None:
        raise HTTPException(status_code=503, detail="connection string with password=secret")

    response = TestClient(local).get("/upstream", headers={"X-Request-ID": "upstream-1"})

    assert response.status_code == 503
    assert response.headers["x-request-id"] == "upstream-1"
    assert response.json() == {
        "error": {
            "code": "service_unavailable",
            "message": "服务暂时不可用，请稍后重试",
            "request_id": "upstream-1",
        }
    }
    assert "secret" not in response.text


def test_domain_error_is_mapped_without_losing_http_detail_compatibility() -> None:
    local = FastAPI()
    local.add_exception_handler(OutputVersionNotFound, handle_not_found_error)
    local.add_exception_handler(HTTPException, handle_http_exception)
    local.middleware("http")(request_id_middleware)

    @local.get("/missing")
    def missing() -> None:
        raise OutputVersionNotFound("output-v1")

    @local.get("/conflict")
    def conflict() -> None:
        raise HTTPException(status_code=409, detail={"code": "already_running", "message": "任务已在运行"})

    client = TestClient(local)
    missing_response = client.get("/missing", headers={"X-Request-ID": "missing-1"})
    conflict_response = client.get("/conflict", headers={"X-Request-ID": "conflict-1"})

    assert missing_response.status_code == 404
    assert missing_response.json()["error"] == {
        "code": "output_version_not_found",
        "message": "output-v1",
        "request_id": "missing-1",
    }
    assert conflict_response.status_code == 409
    assert conflict_response.json()["detail"] == {"code": "already_running", "message": "任务已在运行"}
    assert conflict_response.json()["error"]["code"] == "already_running"


def test_validation_error_handler_returns_structured_error() -> None:
    from pydantic import BaseModel

    class Payload(BaseModel):
        required: int

    local = FastAPI()
    local.add_exception_handler(RequestValidationError, handle_validation_error)

    @local.post("/validate")
    def validate(_payload: Payload) -> dict[str, bool]:
        return {"ok": True}

    response = TestClient(local).post("/validate", json={})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["detail"][0]["type"] == "missing"
