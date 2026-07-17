from __future__ import annotations

import json
import os
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from cube_split.ingest.ray_ingest_job import cell_geometry_geojson
from cube_web.acceptance.m6_mock_data import DATA_TYPES, build_mock_manifest
from cube_web.routes.auth import Actor
from cube_web.routes.m6_datasets import create_m6_datasets_router
from cube_web.routes.m6_ingest_runs import create_m6_ingest_runs_router
from cube_web.routes.m6_scene import create_m6_scene_router
from cube_web.services.m6_dataset_management import DatasetManagementService, InMemoryDatasetManagementRepository
from cube_web.services.m6_ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services.m6_ingest_repository import InMemoryIngestRepository
from cube_web.services.m6_ingest_service import IngestRunService, QualityGateRejected
from cube_web.services.m6_quality_ingest_bridge import (
    DatasetAutoIngestState,
    PartitionSceneOutput,
    plan_ingest_requests,
)
from cube_web.services.m6_scene_service import SceneDomainService
from cube_web.services import publication_service
from cube_web.services import quality_worker
from cube_web.services.publication_service import (
    PublicationPolicyRejected,
    PublishRequest,
    publish_dataset,
    withdraw_publication,
)
from cube_web.services.quality_repository import QualityLease
from grid_core.sdk import CubeEncoderSDK


def _offline_snapshot() -> dict[str, list[dict[str, Any]]]:
    counts = {"optical": 2, "radar": 1, "product": 1, "carbon": 1}
    return {
        data_type: [
            {
                "source_uri": f"s3://cube/cube/source/{data_type}/mock-{index}.{'nc' if data_type == 'carbon' else 'tif'}",
                "size": 1024 + index,
                "etag": f"etag-{data_type}-{index}",
                "last_modified": "2026-06-01T00:00:00+00:00",
                "mock_identity_sha256": f"{DATA_TYPES.index(data_type) + 1:x}" * 64,
            }
            for index in range(count)
        ]
        for data_type, count in counts.items()
    }


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    path = os.getenv("CUBE_M6_MOCK_MANIFEST", "").strip()
    if path:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        assert value["manifest_version"] == "m6-mock-acceptance-v1"
        return value
    return build_mock_manifest(_offline_snapshot())


def test_manifest_covers_scene_dataset_and_load_batch_cardinality(manifest: dict[str, Any]) -> None:
    memberships = manifest["load_batch_scenes"]
    scenes = {row["scene_id"]: row for row in manifest["scenes"]}
    batches_by_scene: dict[str, set[str]] = {}
    batches_by_dataset: dict[str, set[str]] = {}
    for row in memberships:
        batches_by_scene.setdefault(row["scene_id"], set()).add(row["load_batch_id"])
        batches_by_dataset.setdefault(row["dataset_id"], set()).add(row["load_batch_id"])

    assert set(manifest["expected_coverage"]["data_types"]) == set(DATA_TYPES)
    assert set(manifest["expected_coverage"]["scene_outcomes"]) == {
        "completed", "failed", "partial_failure", "retried", "cancelled"
    }
    assert set(manifest["expected_coverage"]["quality_decisions"]) == {"pass", "warn", "fail"}
    assert manifest["expected_coverage"]["publication_lifecycle"] == ["active", "withdrawn"]
    assert manifest["expected_coverage"]["cell_geom_points"] == {"geohash": 5, "mgrs": 5, "isea4h": 7}
    assert len(manifest["load_batches"][0]["datasets"]) > 1
    assert batches_by_scene["scene-optical-shared"] == {"load-batch-a", "load-batch-b"}
    assert batches_by_dataset["dataset-optical"] == {"load-batch-a", "load-batch-b"}
    assert scenes["scene-optical-shared"]["checksum"] == scenes["scene-optical-duplicate"]["checksum"]
    assert len(scenes["scene-optical-shared"]["assets"]) == 2
    assert {row["load_status"] for row in memberships} >= {"succeeded", "duplicate"}


class _ManifestSceneRepository:
    def __init__(self, manifest: dict[str, Any]) -> None:
        self.manifest = manifest
        self.scenes = {row["scene_id"]: row for row in manifest["scenes"]}
        self.batches = {row["load_batch_id"]: row for row in manifest["load_batches"]}

    def list_load_batches(self, *, status=None, keyword=None, limit=100):
        rows = [self._batch(row) for row in self.batches.values()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in row["load_batch_id"].casefold()]
        return rows[:limit]

    def get_load_batch(self, load_batch_id):
        row = self.batches.get(load_batch_id)
        return None if row is None else self._batch(row)

    def list_load_batch_scenes(self, load_batch_id, *, status=None, data_type=None, dataset_id=None):
        rows = []
        for member in self.manifest["load_batch_scenes"]:
            if member["load_batch_id"] != load_batch_id:
                continue
            scene = {**self.scenes[member["scene_id"]], **member}
            scene.update(
                dataset_code=scene["dataset_id"],
                dataset_title=scene["dataset_id"],
                product_type=None,
            )
            if status and member["load_status"] != status:
                continue
            if data_type and scene["data_type"] != data_type:
                continue
            if dataset_id and scene["dataset_id"] != dataset_id:
                continue
            rows.append(scene)
        return rows

    @staticmethod
    def _batch(row):
        return {
            **row,
            "batch_name": row["load_batch_id"],
            "status": "succeeded",
            "scene_count": len(row["scene_ids"]),
            "dataset_count": len(row["datasets"]),
        }


def test_load_batch_api_groups_scenes_by_dataset_and_filters(manifest: dict[str, Any]) -> None:
    app = FastAPI()
    service = SceneDomainService(_ManifestSceneRepository(manifest), workflow=object())
    app.include_router(create_m6_scene_router(service), prefix="/v1")
    client = TestClient(app)

    batch = client.get("/v1/partition/load-batches/load-batch-b/scenes")
    assert batch.status_code == 200
    assert batch.json()["scene_count"] == 4
    assert {row["dataset_id"] for row in batch.json()["datasets"]} == {
        "dataset-optical", "dataset-product", "dataset-carbon"
    }
    duplicate = client.get(
        "/v1/partition/load-batches/load-batch-b/scenes",
        params={"status": "duplicate", "data_type": "optical", "dataset_id": "dataset-optical"},
    )
    assert duplicate.json()["scene_count"] == 1
    assert duplicate.json()["datasets"][0]["scenes"][0]["scene_id"] == "scene-optical-duplicate"


def _ingest_request(
    manifest: dict[str, Any],
    dataset_id: str,
    scene_ids: tuple[str, ...],
    *,
    output_version: str = "v1",
    quality_status: str = "pass",
    allow_warn_auto_ingest: bool = False,
) -> CreateIngestRun:
    batches = {
        scene_id: tuple(
            row["load_batch_id"] for row in manifest["load_batch_scenes"] if row["scene_id"] == scene_id
        )
        for scene_id in scene_ids
    }
    planned = plan_ingest_requests(
        quality_run_id=f"quality-{dataset_id}",
        quality_status=quality_status,
        dataset=DatasetAutoIngestState(
            dataset_id=dataset_id,
            status="active",
            auto_ingest_allowed=True,
            current_output_version=output_version,
            allow_warn_auto_ingest=allow_warn_auto_ingest,
        ),
        partition_scenes=tuple(
            PartitionSceneOutput(
                partition_run_id=f"partition-{dataset_id}",
                scene_id=scene_id,
                dataset_id=dataset_id,
                output_version=output_version,
                status="completed",
                source_load_batch_ids=batches[scene_id],
            )
            for scene_id in scene_ids
        ),
    )
    if len(planned) != 1:
        raise ValueError(f"quality status {quality_status} did not create exactly one ingest request")
    return planned[0]


def test_quality_driven_ingest_partial_retry_cancel_and_idempotency(manifest: dict[str, Any]) -> None:
    ownership = {scene["scene_id"]: scene["dataset_id"] for scene in manifest["scenes"]}
    repository = InMemoryIngestRepository(ownership)
    service = IngestRunService(repository)

    optical_request = _ingest_request(
        manifest, "dataset-optical", ("scene-optical-shared", "scene-optical-duplicate")
    )
    optical = service.schedule_after_quality(optical_request, quality_passed=True)
    assert service.schedule_after_quality(optical_request, quality_passed=True).ingest_run_id == optical.ingest_run_id
    service.start_scene(optical.ingest_run_id, "scene-optical-shared")
    service.complete_scene(optical.ingest_run_id, "scene-optical-shared")
    service.start_scene(optical.ingest_run_id, "scene-optical-duplicate")
    partial = service.fail_scene(optical.ingest_run_id, "scene-optical-duplicate", "mock index failure")
    assert partial.status == "partial_failure"

    radar = service.schedule_after_quality(
        _ingest_request(
            manifest,
            "dataset-radar",
            ("scene-radar",),
            quality_status="warn",
            allow_warn_auto_ingest=True,
        ),
        quality_passed=True,
    )
    radar_failed = service.schedule_after_quality(
        _ingest_request(
            manifest,
            "dataset-radar",
            ("scene-radar",),
            output_version="v2",
            quality_status="warn",
            allow_warn_auto_ingest=True,
        ),
        quality_passed=True,
    )
    service.start_scene(radar_failed.ingest_run_id, "scene-radar")
    assert service.fail_scene(radar_failed.ingest_run_id, "scene-radar", "mock tile failure").status == "failed"
    carbon = service.schedule_after_quality(
        _ingest_request(manifest, "dataset-carbon", ("scene-carbon",)),
        quality_passed=True,
    )
    service.start_scene(carbon.ingest_run_id, "scene-carbon")
    assert service.complete_scene(carbon.ingest_run_id, "scene-carbon").status == "completed"
    product_batches = tuple(
        row["load_batch_id"] for row in manifest["load_batch_scenes"] if row["scene_id"] == "scene-product"
    )
    assert plan_ingest_requests(
        quality_run_id="quality-dataset-product",
        quality_status="fail",
        dataset=DatasetAutoIngestState("dataset-product", "active", True, "v1"),
        partition_scenes=(
            PartitionSceneOutput(
                "partition-dataset-product", "scene-product", "dataset-product", "v1", "completed", product_batches
            ),
        ),
    ) == ()
    with pytest.raises(QualityGateRejected):
        service.schedule_after_quality(
            CreateIngestRun(
                partition_run_id="partition-dataset-product",
                dataset_id="dataset-product",
                scenes=(
                    IngestSceneInput(
                        scene_id="scene-product",
                        output_version="v1",
                        quality_run_id="quality-dataset-product",
                        source_load_batch_ids=product_batches,
                    ),
                ),
            ),
            quality_passed=False,
        )

    app = FastAPI()

    @app.middleware("http")
    async def actor_middleware(request: Request, call_next):
        request.state.actor = Actor(username="admin", role="admin")
        return await call_next(request)

    app.include_router(create_m6_ingest_runs_router(service), prefix="/v1")
    client = TestClient(app)
    retried = client.post(
        f"/v1/ingest-runs/{optical.ingest_run_id}/retry",
        json={"scene_ids": ["scene-optical-duplicate"]},
    )
    assert retried.status_code == 200
    assert {row["scene_id"]: row["status"] for row in retried.json()["scenes"]} == {
        "scene-optical-shared": "completed", "scene-optical-duplicate": "queued"
    }
    service.start_scene(optical.ingest_run_id, "scene-optical-duplicate")
    completed = service.complete_scene(optical.ingest_run_id, "scene-optical-duplicate")
    assert completed.status == "completed"
    assert service.complete_scene(optical.ingest_run_id, "scene-optical-duplicate").status == "completed"

    cancelled = client.post(f"/v1/ingest-runs/{radar.ingest_run_id}/cancel", json={"reason": "mock operator cancel"})
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    page = client.get("/v1/ingest-runs", params={"page_size": 20}).json()
    assert {row["status"] for row in page["items"]} >= {"completed", "failed", "cancelled"}
    assert page["summary"]["scene_count"] == 5


class _QualityBridgeCursor:
    connection: "_QualityBridgeTransaction"

    def __init__(self, transaction: "_QualityBridgeTransaction") -> None:
        self.connection = transaction
        self.rows: list[Any] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT data_type, product_type FROM partition_datasets"):
            self.rows = [{"data_type": "optical", "product_type": None}]
        elif normalized.startswith("SELECT table_name FROM information_schema.tables"):
            self.rows = [(name,) for name in (
                "datasets", "load_batch_scenes", "partition_runs", "partition_run_scenes",
                "ingest_runs", "ingest_run_scenes",
            )]
        elif normalized.startswith("SELECT dataset_id,status,auto_ingest_allowed"):
            self.rows = [{
                "dataset_id": "dataset-auto", "status": "active", "auto_ingest_allowed": True,
                "current_output_version": "v1", "attributes": {},
            }]
        elif normalized.startswith("UPDATE datasets d SET current_output_version"):
            self.rows = []
        elif normalized.startswith("SELECT prs.partition_run_id"):
            self.rows = [
                {"partition_run_id": "partition-auto", "scene_id": scene_id, "dataset_id": "dataset-auto",
                 "output_version": "v1", "status": "completed"}
                for scene_id in ("scene-auto-a", "scene-auto-b")
            ]
        elif normalized.startswith("SELECT scene_id,load_batch_id FROM load_batch_scenes"):
            self.rows = [
                {"scene_id": "scene-auto-a", "load_batch_id": "load-batch-a"},
                {"scene_id": "scene-auto-b", "load_batch_id": "load-batch-b"},
            ]
        elif normalized.startswith("SELECT idempotency_key FROM ingest_run_scenes"):
            self.rows = []
        elif normalized.startswith("INSERT INTO ingest_runs"):
            self.connection.ingest_runs.append(params)
            self.rows = []
        elif normalized.startswith("INSERT INTO ingest_run_scenes"):
            self.connection.ingest_scenes.append(params)
            self.rows = []
        else:
            raise AssertionError(f"unexpected quality/ingest bridge SQL: {normalized}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _QualityBridgeTransaction:
    def __init__(self) -> None:
        self.ingest_runs: list[tuple[Any, ...]] = []
        self.ingest_scenes: list[tuple[Any, ...]] = []

    def cursor(self, **_kwargs):
        return _QualityBridgeCursor(self)

    def transaction(self):
        return nullcontext()


class _QualityBridgeStore:
    def __init__(self, transaction: _QualityBridgeTransaction) -> None:
        self.tx = transaction

    def transaction(self):
        return nullcontext(self.tx)


def test_quality_worker_persists_auto_ingest_intent_through_production_bridge(monkeypatch) -> None:
    tx = _QualityBridgeTransaction()
    quality_run_id = UUID("00000000-0000-0000-0000-000000000020")
    monkeypatch.setattr(quality_worker, "require_open_gauss_domain_store", lambda: _QualityBridgeStore(tx))
    monkeypatch.setattr(
        quality_worker,
        "start_quality_run",
        lambda *_args, **_kwargs: SimpleNamespace(
            dataset_id="dataset-auto", output_version="v1", rule_snapshot=()
        ),
    )
    monkeypatch.setattr(quality_worker, "assert_quality_result_totals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(quality_worker, "complete_quality_run_if_current", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(quality_worker, "_m6_auto_ingest_enabled", lambda: True)

    quality_worker.execute_quality_run(QualityLease(quality_run_id, "acceptance-worker", 1))

    assert len(tx.ingest_runs) == 1
    assert tx.ingest_runs[0][1:4] == ("partition-auto", "dataset-auto", "system:quality-gate")
    assert len(tx.ingest_scenes) == 2
    assert {row[1] for row in tx.ingest_scenes} == {"scene-auto-a", "scene-auto-b"}
    assert {row[3] for row in tx.ingest_scenes} == {"v1"}


def _management_fixture(manifest: dict[str, Any]):
    datasets = {
        row["dataset_id"]: {
            "dataset_id": row["dataset_id"],
            "dataset_code": row["dataset_id"],
            "dataset_title": row["dataset_id"],
            "data_type": row["data_type"],
            "product_type": None,
            "status": "active",
            "created_at": "2026-06-01T00:00:00Z",
            "updated_at": "2026-06-01T00:00:00Z",
        }
        for row in manifest["datasets"]
    }
    details = {dataset_id: {"scenes": [], "quality": [], "publications": []} for dataset_id in datasets}
    for scene in manifest["scenes"]:
        details[scene["dataset_id"]]["scenes"].append(dict(scene))
    for dataset_id, decision in manifest["quality_decisions"].items():
        details[dataset_id]["quality"].append({"quality_run_id": f"quality-{dataset_id}", "status": decision})
    repository = InMemoryDatasetManagementRepository(datasets=datasets, details=details)
    return repository, DatasetManagementService(repository)


def test_dataset_management_quality_views_reassignment_and_provenance(manifest: dict[str, Any]) -> None:
    repository, service = _management_fixture(manifest)
    app = FastAPI()

    @app.middleware("http")
    async def actor_middleware(request: Request, call_next):
        request.state.actor = Actor(username="admin", role="admin")
        return await call_next(request)

    app.include_router(create_m6_datasets_router(service), prefix="/v1")
    client = TestClient(app)

    overview = client.get("/v1/datasets", params={"page_size": 20}).json()
    assert overview["summary"]["dataset_count"] == 5
    assert overview["summary"]["scene_count"] == 5
    assert {row["quality_status"] for row in overview["items"]} >= {"pass", "warn", "fail"}
    reassigned = client.post(
        "/v1/datasets/dataset-product/scenes/scene-product/reassign",
        json={"target_dataset_id": "dataset-product-target", "reason": "correct product family"},
    )
    assert reassigned.status_code == 200
    provenance = client.get("/v1/datasets/dataset-product-target/provenance").json()["items"]
    assert any(
        row["scene_id"] == "scene-product"
        and row["previous_dataset_id"] == "dataset-product"
        and row["dataset_id"] == "dataset-product-target"
        for row in provenance
    )
    assert client.get("/v1/datasets/dataset-product/scenes").json()["total"] == 0
    assert client.get("/v1/datasets/dataset-product-target/scenes").json()["total"] == 1
    assert repository.scene_audit[-1]["changed_by"] == "admin"


class _PublicationCursor:
    def __init__(self, store: "_PublicationStore") -> None:
        self.store = store
        self.row: dict[str, Any] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT status FROM partition_output_versions"):
            self.row = {"status": "completed"}
        elif normalized.startswith("SELECT rule_set_version FROM partition_quality_warn_approvals"):
            self.row = {"rule_set_version": "rules-v1"} if self.store.warn_approved else None
        elif normalized.startswith("SELECT * FROM partition_publications WHERE dataset_id"):
            self.row = self.store.publication if self.store.publication and self.store.publication["status"] == "active" else None
        elif normalized.startswith("INSERT INTO partition_publications"):
            now = datetime.now(UTC)
            self.store.publication = {
                "publication_id": params[0],
                "dataset_id": self.store.dataset_id,
                "output_version": self.store.output_version,
                "quality_run_id": self.store.quality_run_id,
                "status": "active",
                "service_version_id": params[4],
                "requested_by": params[5],
                "requested_at": now,
                "activated_at": now,
                "failure": None,
                "withdrawn_by": None,
                "withdrawn_at": None,
                "withdrawal_reason": None,
            }
            self.row = dict(self.store.publication)
        elif normalized.startswith("SELECT * FROM partition_publications WHERE publication_id"):
            self.row = dict(self.store.publication) if self.store.publication else None
        elif normalized.startswith("UPDATE partition_publications SET status = 'withdrawn'"):
            assert self.store.publication is not None
            self.store.publication.update(
                status="withdrawn",
                withdrawn_by=params[0],
                withdrawn_at=datetime.now(UTC),
                withdrawal_reason=params[1],
            )
            self.row = dict(self.store.publication)
        else:
            raise AssertionError(f"unexpected publication SQL: {normalized}")

    def fetchone(self):
        return self.row


class _PublicationStore:
    dataset_id = "dataset-publication"
    output_version = "v1"
    quality_run_id = UUID("00000000-0000-0000-0000-000000000010")

    def __init__(self) -> None:
        self.warn_approved = False
        self.publication: dict[str, Any] | None = None

    def transaction(self):
        return nullcontext(self)

    def cursor(self, **_kwargs):
        return _PublicationCursor(self)


@pytest.mark.parametrize("quality_status", ["pass", "warn", "fail"])
def test_production_publication_policy_and_withdrawal(monkeypatch, quality_status: str) -> None:
    store = _PublicationStore()
    monkeypatch.setattr(publication_service, "require_open_gauss_domain_store", lambda: store)
    monkeypatch.setattr(
        publication_service,
        "lock_dataset",
        lambda _tx, _dataset_id: {
            "dataset_id": store.dataset_id,
            "current_output_version": store.output_version,
            "current_quality_run_id": store.quality_run_id,
        },
    )
    monkeypatch.setattr(
        publication_service,
        "lock_quality_run",
        lambda _tx, _quality_run_id: {
            "quality_run_id": store.quality_run_id,
            "dataset_id": store.dataset_id,
            "output_version": store.output_version,
            "status": quality_status,
            "result_complete": True,
            "rule_set_version": "rules-v1",
        },
    )
    actor = Actor("admin", "admin")

    if quality_status == "fail":
        with pytest.raises(PublicationPolicyRejected):
            publish_dataset(store.dataset_id, PublishRequest(), actor)
        return
    if quality_status == "warn":
        with pytest.raises(PublicationPolicyRejected, match="requires approval"):
            publish_dataset(store.dataset_id, PublishRequest(), actor)
        store.warn_approved = True

    publication = publish_dataset(store.dataset_id, PublishRequest(), actor)
    assert publication.status == "active"
    assert publish_dataset(store.dataset_id, PublishRequest(), actor).publication_id == publication.publication_id
    withdrawn = withdraw_publication(store.dataset_id, publication.publication_id, "acceptance withdrawal", actor)
    assert withdrawn.status == "withdrawn"
    assert withdrawn.withdrawal_reason == "acceptance withdrawal"


def test_cell_geom_uses_closed_real_sdk_boundaries_for_every_required_grid() -> None:
    sdk = CubeEncoderSDK()
    for grid_type, level, expected_points in (("geohash", 5, 5), ("mgrs", 3, 5), ("isea4h", 2, 7)):
        cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=[116.30, 39.88])
        geometry = json.loads(
            cell_geometry_geojson(
                grid_type=grid_type,
                grid_level=cell.grid_level,
                space_code=cell.space_code,
                topology_code=cell.topology_code,
                sdk=sdk,
            )
        )
        ring = geometry["coordinates"][0]
        assert geometry["type"] == "Polygon"
        assert len(ring) == expected_points
        assert ring[0] == ring[-1]
        assert len({tuple(point) for point in ring[:-1]}) == expected_points - 1
        assert all(-180 <= lon <= 180 and -90 <= lat <= 90 for lon, lat in ring)
        if grid_type == "isea4h":
            assert len({point[0] for point in ring[:-1]}) > 2
            assert len({point[1] for point in ring[:-1]}) > 2
