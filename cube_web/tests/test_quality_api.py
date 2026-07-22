from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import cube_web.routes.quality as quality_routes
from cube_web.routes.auth import Actor
from cube_web.routes.quality import create_quality_router


def test_only_normalized_quality_routes_exist() -> None:
    paths = {route.path for route in create_quality_router().routes}
    assert paths == {
        "/quality/records",
        "/quality/records/{quality_run_id}",
        "/quality/records/{quality_run_id}/results",
        "/quality/records/{quality_run_id}/errors",
        "/quality/records/{quality_run_id}/errors/export",
        "/quality/rules",
        "/quality/rules/settings",
        "/quality/rules/{rule_code}/enabled",
        "/quality/runs",
    }
    assert not any(segment in path for path in paths for segment in ("/history", "/latest", "/report", "/optical", "/radar", "/product", "/carbon"))


def test_rule_catalog_exposes_requirement_and_product_applicability() -> None:
    route = next(route for route in create_quality_router().routes if route.path == "/quality/rules")
    body = route.endpoint()
    rules = {item["code"]: item for item in body["items"]}

    assert body["rule_set_version"]
    assert rules["asset_readability"]["mandatory"] is True
    assert rules["asset_crs"]["toggleable"] is True
    assert rules["asset_crs"]["mandatory"] is False
    assert not {"metadata_completeness", "declared_metadata_defects", "declared_metadata_warnings", "pixel_sample"} & set(rules)
    assert "product_year_consistency" not in rules
    assert rules["carbon_schema"]["applicability"]["data_types"] == ["carbon"]


def test_rule_settings_require_admin_and_toggle_one_optional_rule(monkeypatch) -> None:
    saved: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        quality_routes,
        "set_optional_quality_rule_enabled",
        lambda code, enabled: saved.append((code, enabled)) or ("carbon_schema",),
    )
    app = FastAPI()

    @app.middleware("http")
    async def actor(request: Request, call_next):
        role = request.headers.get("x-test-role", "viewer")
        request.state.actor = Actor(username=role, role=role)
        return await call_next(request)

    app.include_router(create_quality_router(), prefix="/v1")
    client = TestClient(app)

    assert client.put("/v1/quality/rules/carbon_schema/enabled", json={"enabled": False}).status_code == 403
    response = client.put(
        "/v1/quality/rules/carbon_schema/enabled",
        json={"enabled": True},
        headers={"x-test-role": "admin"},
    )

    assert response.status_code == 200
    assert saved == [("carbon_schema", True)]
    rules = {item["code"]: item for item in response.json()["items"]}
    assert rules["index_schema"]["enabled"] is True
    assert rules["carbon_schema"]["enabled"] is True
