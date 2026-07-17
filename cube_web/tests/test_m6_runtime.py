from __future__ import annotations

import logging

from fastapi import APIRouter

import cube_web.app as web_app
from cube_web.services.m6_runtime import m6_runtime_policy


class _SceneService:
    pass


def _fake_m6_components():
    scene = _SceneService()
    scene_router = APIRouter(prefix="/partition")
    dataset_router = APIRouter(prefix="/datasets")
    ingest_router = APIRouter(prefix="/ingest-runs")

    @scene_router.get("/load-batches")
    def load_batches():
        return []

    @scene_router.post("/runs")
    def create_run():
        return {}

    @dataset_router.get("")
    def datasets():
        return []

    @dataset_router.patch("/{dataset_id}")
    def update_dataset(dataset_id: str):
        return {"dataset_id": dataset_id}

    @ingest_router.get("")
    def ingest_runs():
        return []

    @ingest_router.post("/{ingest_run_id}/cancel")
    def cancel_ingest(ingest_run_id: str):
        return {"ingest_run_id": ingest_run_id}

    return scene, (scene_router, dataset_router, ingest_router)


def _route_methods(app):
    return {
        (route.path, method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }


def test_m6_mode_defaults_to_legacy(monkeypatch) -> None:
    monkeypatch.delenv("CUBE_WEB_M6_MODE", raising=False)
    monkeypatch.setattr(
        "cube_web.services.m6_runtime.runtime_config.env_text",
        lambda key, default=None: default,
    )
    monkeypatch.setattr(web_app, "_build_m6_components", lambda: (_ for _ in ()).throw(AssertionError("M6 built")))

    routes = _route_methods(web_app.create_app())

    assert ("/v1/partition/batches", "GET") in routes
    assert ("/v1/partition/load-batches", "GET") not in routes
    assert ("/v1/datasets", "GET") not in routes
    assert ("/v1/ingest-runs", "GET") not in routes


def test_m6_read_exposes_only_query_routes(monkeypatch) -> None:
    monkeypatch.setattr(web_app, "_build_m6_components", _fake_m6_components)

    routes = _route_methods(web_app.create_app(m6_mode="m6-read"))

    assert ("/v1/partition/load-batches", "GET") in routes
    assert ("/v1/datasets", "GET") in routes
    assert ("/v1/ingest-runs", "GET") in routes
    assert ("/v1/partition/runs", "POST") not in routes
    assert ("/v1/datasets/{dataset_id}", "PATCH") not in routes
    assert ("/v1/ingest-runs/{ingest_run_id}/cancel", "POST") not in routes


def test_m6_primary_exposes_writes_and_passes_scene_service_to_import(monkeypatch) -> None:
    scene_service, routers = _fake_m6_components()
    monkeypatch.setattr(web_app, "_build_m6_components", lambda: (scene_service, routers))
    captured = {}
    real_create_partition_router = web_app.create_partition_router

    def capture_partition_router(*args, **kwargs):
        captured["scene_service"] = kwargs.get("scene_service")
        return real_create_partition_router(*args, **kwargs)

    monkeypatch.setattr(web_app, "create_partition_router", capture_partition_router)

    routes = _route_methods(web_app.create_app(m6_mode="m6-primary"))

    assert captured["scene_service"] is scene_service
    assert ("/v1/partition/runs", "POST") in routes
    assert ("/v1/datasets/{dataset_id}", "PATCH") in routes
    assert ("/v1/ingest-runs/{ingest_run_id}/cancel", "POST") in routes


def test_shadow_keeps_legacy_writes_without_building_m6(monkeypatch) -> None:
    monkeypatch.setattr(web_app, "_build_m6_components", lambda: (_ for _ in ()).throw(AssertionError("M6 built")))

    routes = _route_methods(web_app.create_app(m6_mode="shadow"))

    assert ("/v1/partition/schemas/import", "POST") in routes
    assert ("/v1/partition/load-batches", "GET") not in routes
    assert ("/v1/partition/runs", "POST") not in routes


def test_invalid_m6_mode_fails_closed(monkeypatch, caplog) -> None:
    monkeypatch.setattr(web_app, "_build_m6_components", lambda: (_ for _ in ()).throw(AssertionError("M6 built")))
    caplog.set_level(logging.ERROR, logger="cube_web.services.m6_runtime")

    policy = m6_runtime_policy("unexpected")
    routes = _route_methods(web_app.create_app(m6_mode="unexpected"))

    assert policy.mode == "legacy"
    assert ("/v1/partition/load-batches", "GET") not in routes
    assert "falling back to legacy" in caplog.text
