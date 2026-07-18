from __future__ import annotations

from fastapi import APIRouter

import cube_web.app as web_app


class _SceneService:
    pass


def _formal_components():
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

    @ingest_router.get("")
    def ingest_runs():
        return []

    return scene, (scene_router, dataset_router, ingest_router)


def _route_methods(app):
    return {
        (route.path, method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }


def test_formal_domain_routes_are_always_mounted(monkeypatch) -> None:
    scene_service, routers = _formal_components()
    monkeypatch.setattr(web_app, "_build_domain_components", lambda: (scene_service, routers))
    captured = {}
    real_factory = web_app.create_partition_router

    def capture_router(*args, **kwargs):
        captured["scene_service"] = kwargs.get("scene_service")
        return real_factory(*args, **kwargs)

    monkeypatch.setattr(web_app, "create_partition_router", capture_router)
    routes = _route_methods(web_app.create_app())

    assert captured["scene_service"] is scene_service
    assert ("/v1/partition/load-batches", "GET") in routes
    assert ("/v1/partition/runs", "POST") in routes
    assert ("/v1/datasets", "GET") in routes
    assert ("/v1/ingest-runs", "GET") in routes


def test_retired_partition_routes_are_absent(monkeypatch) -> None:
    monkeypatch.setattr(web_app, "_build_domain_components", _formal_components)
    routes = _route_methods(web_app.create_app())

    retired = {
        ("/v1/partition/batches", "GET"),
        ("/v1/partition/tasks/run", "POST"),
        ("/v1/partition/schemas/reconcile", "POST"),
        ("/v1/partition/optical/demo", "POST"),
        ("/v1/partition/optical/test", "POST"),
    }
    assert routes.isdisjoint(retired)
