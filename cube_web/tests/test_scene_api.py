from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from cube_split.partition.carbon import CarbonSatelliteObservation
from cube_web.routes.auth import Actor
from cube_web.routes.partition import import_partition_schema_payload
from cube_web.routes.scene_partition import create_scene_partition_router
from cube_web.schemas import PartitionSchemaImportRequest
from cube_web.services.partition_contracts import DatasetInput
from cube_web.services.scene_contracts import ScenePartitionRunRequest
from cube_web.services.scene_repository import (
    OpenGaussSceneRepository,
    _load_schema_datasets,
    _partition_scene_idempotency_key,
)
from cube_web.services.scene_service import SceneDomainService, build_partition_execution_request


def _dataset(dataset_id: str, data_type: str, scene_id: str) -> DatasetInput:
    carbon = data_type == "carbon"
    uri = f"s3://cube/source/{scene_id}.nc" if carbon else f"s3://cube/source/{scene_id}.tif"
    asset: dict[str, Any] = {
        "source_asset_id": f"asset-{scene_id}",
        "checksum": "a" * 64,
        "time_start": "2026-07-01T00:00:00Z",
        "time_end": "2026-07-01T01:00:00Z",
        "attributes": {"scene_id": scene_id},
    }
    if carbon:
        asset.update({"source_uri": uri, "source_kind": "raw", "source_format": "netcdf"})
    else:
        asset.update({"cog_uri": uri, "bbox": [100, 20, 101, 21], "crs": "EPSG:4326"})
    return DatasetInput.model_validate(
        {
            "dataset_id": dataset_id,
            "dataset_code": dataset_id.upper(),
            "dataset_title": dataset_id,
            "data_type": data_type,
            "assets": [asset],
            "bands": [
                {
                    "source_asset_id": f"asset-{scene_id}",
                    "band_code": "XCO2" if carbon else "B04",
                    "band_name": "XCO2" if carbon else "Red",
                    "band_type": "variable" if carbon else "spectral",
                    "display_order": 0,
                }
            ],
        }
    )


def _payload() -> dict[str, Any]:
    return {
        "partition_run_id": "partition-run-001",
        "source_batch_ids": ["load-001"],
        "datasets": [
            {
                "dataset_id": "dataset-optical",
                "scene_ids": ["scene-optical"],
                "band_unit_ids": ["band-scene-optical-b04"],
                "partition": {
                    "grid_type": "isea4h",
                    "requested_grid_level": 1,
                    "partition_method": "entity",
                    "cover_mode": "intersect",
                },
            },
            {
                "dataset_id": "dataset-carbon",
                "scene_ids": ["scene-carbon"],
                "band_unit_ids": ["band-scene-carbon-xco2"],
                "partition": {
                    "grid_type": "isea4h",
                    "requested_grid_level": 1,
                    "partition_method": "entity",
                    "max_observations": 50,
                },
            },
        ],
    }


class _Task:
    task_id = "partition-task-001"

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": "queued",
            "data_type": "mixed",
            "operation": "run",
        }


class _Workflow:
    def __init__(self) -> None:
        self.request = None

    def submit_mixed(self, request):
        self.request = request
        return _Task()

    def submit_strict(self, data_type, request):
        self.request = request
        return _Task()


class _Repository:
    def __init__(self) -> None:
        self.bound = None
        self.run_request = None
        self.fail_materialize = False
        self.existing_run = None
        self.failed_run = None
        self.drafts = {}

    def list_load_batches(self, **_kwargs):
        return [{"load_batch_id": "load-001", "batch_name": "Loader batch", "scene_count": 2}]

    def get_load_batch(self, load_batch_id):
        if load_batch_id != "load-001":
            return None
        return {"load_batch_id": load_batch_id, "batch_name": "Loader batch"}

    def list_load_batch_scenes(self, _load_batch_id, **_kwargs):
        return [
            {
                "scene_id": "scene-optical",
                "dataset_id": "dataset-optical",
                "dataset_code": "OPTICAL",
                "dataset_title": "Optical",
                "data_type": "optical",
            },
            {
                "scene_id": "scene-carbon",
                "dataset_id": "dataset-carbon",
                "dataset_code": "CARBON",
                "dataset_title": "Carbon",
                "data_type": "carbon",
                "product_type": "tansat",
                "source_uri": "s3://cube/source/scene-carbon.nc",
                "load_status": "succeeded",
            },
        ]

    def list_carbon_preview_sources(self, source_batch_ids, scene_ids):
        if tuple(source_batch_ids) != ("load-001",) or tuple(scene_ids) != ("scene-carbon",):
            return []
        return [{
            "load_batch_id": "load-001",
            "scene_id": "scene-carbon",
            "source_uri": "s3://cube/source/load-001/scene-carbon.nc",
            "product_type": "tansat",
        }]

    def materialize_partition_datasets(self, request):
        if self.fail_materialize:
            raise ValueError("scenes are not linked to source_batch_ids: ['scene-carbon']")
        if len(request.datasets) == 1:
            selection = request.datasets[0]
            if selection.dataset_id == "dataset-optical":
                return (_dataset("dataset-optical", "optical", "scene-optical"),)
            return (_dataset("dataset-carbon", "carbon", "scene-carbon"),)
        return (
            _dataset("dataset-optical", "optical", "scene-optical"),
            _dataset("dataset-carbon", "carbon", "scene-carbon"),
        )

    def create_partition_draft(self, *, draft_id, draft_name, data_type, source_batch_ids, selection, created_by):
        draft = {
            "draft_id": draft_id, "draft_name": draft_name, "data_type": data_type, "source_load_batch_ids": list(source_batch_ids),
            "selection": selection, "status": "pending", "created_by": created_by,
        }
        self.drafts[draft_id] = draft
        return draft

    def list_partition_drafts(self, *, data_type=None, limit=100):
        return [draft for draft in self.drafts.values() if draft["status"] == "pending" and (data_type is None or draft["data_type"] == data_type)][:limit]

    def mark_partition_draft_submitted(self, draft_id, partition_run_id):
        draft = self.drafts.get(draft_id)
        if draft is None or draft["status"] != "pending":
            return None
        draft.update(status="submitted", submitted_partition_run_id=partition_run_id)
        return draft

    def create_partition_run(self, request):
        self.run_request = request
        if self.existing_run is not None:
            return self.existing_run
        return {"partition_run_id": request.partition_run_id, "status": "pending", "created": True}

    def bind_partition_task(self, partition_run_id, task_id):
        self.bound = (partition_run_id, task_id)

    def fail_partition_run(self, partition_run_id, error_message):
        self.failed_run = (partition_run_id, error_message)

    def list_partition_quality_batches(self, **_kwargs):
        return [{"partition_run_id": "partition-run-001", "band_count": 2, "quality_pass_count": 1}]

    def get_partition_quality_batch(self, partition_run_id):
        if partition_run_id != "partition-run-001":
            return None
        return {
            "partition_run_id": partition_run_id,
            "source_load_batch_ids": ["load-001", "load-002"],
            "summary": {"band_count": 2, "quality_pass_count": 1},
            "datasets": [{"dataset_id": "dataset-optical", "scenes": [{"scene_id": "scene-optical", "bands": []}]}],
        }

    def list_partition_quality_targets(self, partition_run_id):
        return [{"dataset_id": "dataset-optical", "output_version": "output-001"}] if partition_run_id == "partition-run-001" else []


@pytest.fixture
def api():
    repository = _Repository()
    workflow = _Workflow()
    app = FastAPI()

    @app.middleware("http")
    async def actor(request: Request, call_next):
        role = request.headers.get("x-test-role", "admin")
        request.state.actor = Actor(username=role, role=role)
        return await call_next(request)

    app.include_router(
        create_scene_partition_router(
            SceneDomainService(repository, workflow, quality_requester=lambda dataset_id, output_version, actor: {
                "dataset_id": dataset_id, "output_version": output_version, "requested_by": actor.username,
            })
        ),
        prefix="/v1",
    )
    return TestClient(app), repository, workflow


def test_load_batch_scenes_are_grouped_by_dataset(api) -> None:
    client, _, _ = api

    response = client.get("/v1/partition/load-batches/load-001/scenes")

    assert response.status_code == 200
    body = response.json()
    assert body["scene_count"] == 2
    assert [item["dataset_id"] for item in body["datasets"]] == ["dataset-optical", "dataset-carbon"]
    assert body["datasets"][0]["scenes"][0]["scene_id"] == "scene-optical"


def test_carbon_footprint_preview_reads_selected_source_scene(api, monkeypatch, tmp_path) -> None:
    client, _, _ = api
    source_path = tmp_path / "tansat.nc"
    source_path.write_bytes(b"fixture")
    source_uris: list[str] = []

    def resolve_source(source_uri, *_args, **_kwargs):
        source_uris.append(source_uri)
        return str(source_path)

    monkeypatch.setattr("cube_web.services.scene_service.resolve_asset_source_path", resolve_source)
    monkeypatch.setattr(
        "cube_web.services.scene_service.load_observations_from_file",
        lambda *_args, **_kwargs: [
            CarbonSatelliteObservation(
                satellite="TanSat", observation_id="obs-1", acq_time="2026-07-01T00:00:00Z",
                lon=100.5, lat=20.5, xco2=420.0,
                footprint=[[100.0, 20.0], [101.0, 20.0], [100.5, 21.0]], source_index=3,
            ),
            CarbonSatelliteObservation(
                satellite="TanSat", observation_id="obs-2", acq_time="2026-07-01T00:00:01Z",
                lon=100.75, lat=20.25, xco2=421.0, source_index=4,
            ),
        ],
    )

    response = client.post("/v1/partition/carbon/footprints", json={
        "source_batch_ids": ["load-001"], "scene_ids": ["scene-carbon"], "limit": 50,
    })

    assert response.status_code == 200
    assert source_uris == ["s3://cube/source/load-001/scene-carbon.nc"]
    assert response.json() == {
        "items": [{
            "scene_id": "scene-carbon", "source_batch_id": "load-001", "observation_id": "obs-1", "source_index": 3,
            "geometry": {"type": "Polygon", "coordinates": [[[100.0, 20.0], [101.0, 20.0], [100.5, 21.0], [100.0, 20.0]]]},
        }, {
            "scene_id": "scene-carbon", "source_batch_id": "load-001", "observation_id": "obs-2", "source_index": 4,
            "geometry": {"type": "Point", "coordinates": [100.75, 20.25]},
        }],
        "truncated": False,
        "unavailable_sources": [],
    }


def test_carbon_footprint_preview_skips_unavailable_source(api, monkeypatch) -> None:
    client, _, _ = api

    def missing_source(*_args, **_kwargs):
        raise FileNotFoundError("source object is missing")

    monkeypatch.setattr("cube_web.services.scene_service.resolve_asset_source_path", missing_source)

    response = client.post("/v1/partition/carbon/footprints", json={
        "source_batch_ids": ["load-001"], "scene_ids": ["scene-carbon"], "limit": 50,
    })

    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "truncated": False,
        "unavailable_sources": [{
            "scene_id": "scene-carbon",
            "source_batch_id": "load-001",
            "reason": "source_not_found",
        }],
    }


def test_carbon_grid_preview_covers_observation_footprints(api, monkeypatch, tmp_path) -> None:
    client, _, _ = api
    source_path = tmp_path / "tansat.nc"
    source_path.write_bytes(b"fixture")
    monkeypatch.setattr("cube_web.services.scene_service.resolve_asset_source_path", lambda *_args, **_kwargs: str(source_path))
    monkeypatch.setattr(
        "cube_web.services.scene_service.load_observations_from_file",
        lambda *_args, **_kwargs: [
            CarbonSatelliteObservation(
                satellite="TanSat", observation_id="obs-1", acq_time="2026-07-01T00:00:00Z",
                lon=100.5, lat=20.5, xco2=420.0,
                footprint=[[100.0, 20.0], [101.0, 20.0], [100.5, 21.0]], source_index=3,
            ),
        ],
    )

    response = client.post("/v1/partition/carbon/grid-preview", json={
        "source_batch_ids": ["load-001"], "scene_ids": ["scene-carbon"],
        "grid_type": "isea4h", "requested_grid_level": 1,
    })

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["geometry"]["type"] == "Polygon"
    assert body["cells"]
    assert body["cells"][0]["grid_type"] == "isea4h"
    assert body["cell_limit_reached"] is False


def test_scene_partition_run_uses_distinct_run_and_source_batch_ids(api) -> None:
    client, repository, workflow = api

    response = client.post("/v1/partition/runs", json=_payload())

    assert response.status_code == 202
    assert response.json() == {
        "partition_run_id": "partition-run-001",
        "source_batch_ids": ["load-001"],
        "task_id": "partition-task-001",
        "status": "queued",
        "data_type": "mixed",
        "operation": "run",
    }
    assert workflow.request.batch_id == "partition-run-001"
    assert "load-001" not in workflow.request.batch_id
    assert repository.bound == ("partition-run-001", "partition-task-001")
    assert repository.run_request.datasets[0].band_unit_ids == ("band-scene-optical-b04",)
    assert [dataset.dataset_id for dataset in workflow.request.datasets] == ["dataset-optical", "dataset-carbon"]
    assert workflow.request.datasets[1].partition.grid_type == "isea4h"


def test_scene_partition_run_requires_admin(api) -> None:
    client, repository, workflow = api

    response = client.post("/v1/partition/runs", json=_payload(), headers={"x-test-role": "user"})

    assert response.status_code == 403
    assert repository.run_request is None
    assert workflow.request is None


def test_scene_partition_run_rejects_scene_outside_selected_load_batches(api) -> None:
    client, repository, _ = api
    repository.fail_materialize = True

    response = client.post("/v1/partition/runs", json=_payload())

    assert response.status_code == 422
    assert "not linked to source_batch_ids" in response.json()["detail"]
    assert repository.run_request is None


def test_scene_partition_run_replay_returns_original_task_without_resubmit(api) -> None:
    client, repository, workflow = api
    repository.existing_run = {
        "partition_run_id": "partition-run-001",
        "status": "queued",
        "created": False,
        "attributes": {"task_id": "partition-task-existing"},
    }

    response = client.post("/v1/partition/runs", json=_payload())

    assert response.status_code == 202
    assert response.json()["task_id"] == "partition-task-existing"
    assert workflow.request is None
    assert repository.bound is None


def test_partition_quality_is_grouped_by_partition_run_and_can_start_dataset_quality(api) -> None:
    client, _, _ = api

    listed = client.get("/v1/partition/runs")
    detail = client.get("/v1/partition/runs/partition-run-001/quality")
    submitted = client.post("/v1/partition/runs/partition-run-001/quality")

    assert listed.status_code == 200
    assert listed.json()["items"][0]["partition_run_id"] == "partition-run-001"
    assert detail.json()["source_load_batch_ids"] == ["load-001", "load-002"]
    assert detail.json()["datasets"][0]["scenes"][0]["scene_id"] == "scene-optical"
    assert submitted.status_code == 202
    assert submitted.json()["quality_runs"] == [{
        "dataset_id": "dataset-optical", "output_version": "output-001", "requested_by": "admin",
    }]


def test_data_management_selection_creates_a_pending_partition_draft(api) -> None:
    client, _, _ = api
    payload = {
        "data_type": "optical",
        "draft_name": "光学验收剖分批次",
        "source_batch_ids": ["load-001"],
        "datasets": [{
            "dataset_id": "dataset-optical",
            "band_unit_ids": ["band-scene-optical-b04"],
            "scenes": [{"scene_id": "scene-optical", "source_batch_ids": ["load-001"]}],
            "partition": {
                "grid_type": "isea4h", "requested_grid_level": 1,
                "partition_method": "entity", "cover_mode": "intersect",
            },
        }],
    }

    created = client.post("/v1/partition/drafts", json=payload)
    assert created.status_code == 201
    draft_id = created.json()["draft_id"]
    assert created.json()["draft_name"] == "光学验收剖分批次"
    assert client.get("/v1/partition/drafts?data_type=optical").json()["items"][0]["draft_id"] == draft_id

    submitted = client.post(f"/v1/partition/drafts/{draft_id}/submitted", json={"partition_run_id": "partition-run-001"})
    assert submitted.status_code == 200
    assert submitted.json()["status"] == "submitted"
    assert client.get("/v1/partition/drafts?data_type=optical").json()["items"] == []


def test_scene_partition_run_submission_failure_releases_claim(api) -> None:
    client, repository, workflow = api
    workflow.submit_mixed = lambda _request: (_ for _ in ()).throw(RuntimeError("queue unavailable"))

    with pytest.raises(RuntimeError, match="queue unavailable"):
        client.post("/v1/partition/runs", json=_payload())

    assert repository.failed_run == ("partition-run-001", "queue unavailable")


def test_scene_service_projects_workflow_task_events() -> None:
    class Workflow:
        def add_task_event_listener(self, listener):
            self.listener = listener

    repository = _Repository()
    repository.events = []
    repository.update_partition_task = lambda task_id, status, result=None: repository.events.append((task_id, status, result))
    workflow = Workflow()
    SceneDomainService(repository, workflow)

    workflow.listener("partition-task-001", "partial_failure", {"datasets": []})

    assert repository.events == [("partition-task-001", "partial_failure", {"datasets": []})]


def test_scene_service_rebinds_retry_through_task_ancestry() -> None:
    class Store:
        attempts = {
            "partition-task-retry-1": {"source_task_id": "partition-task-original"},
        }

        def get_attempt(self, task_id):
            return self.attempts.get(task_id)

    class CurrentTask:
        def to_dict(self):
            return {"task_id": "partition-task-retry-2", "status": "completed", "result": {"datasets": []}}

    class Workflow:
        store = Store()

        def get_task(self, task_id):
            assert task_id == "partition-task-retry-2"
            return CurrentTask()

    class Repository:
        def __init__(self):
            self.rebinds = []
            self.events = []

        def rebind_partition_task(self, source_task_id, task_id):
            self.rebinds.append((source_task_id, task_id))
            return "partition-run-001" if source_task_id == "partition-task-original" else None

        def update_partition_task(self, task_id, status, result=None):
            self.events.append((task_id, status, result))

    repository = Repository()
    service = SceneDomainService(repository, Workflow())

    partition_run_id = service.bind_partition_retry("partition-task-retry-1", "partition-task-retry-2")

    assert partition_run_id == "partition-run-001"
    assert repository.rebinds == [
        ("partition-task-retry-1", "partition-task-retry-2"),
        ("partition-task-original", "partition-task-retry-2"),
    ]
    assert repository.events == [
        ("partition-task-retry-2", "completed", {"datasets": []}),
    ]


def test_scene_service_retry_event_recovers_a_missed_initial_rebind() -> None:
    class Store:
        def get_attempt(self, task_id):
            assert task_id == "partition-task-retry"
            return {"source_task_id": "partition-task-original"}

    class CurrentTask:
        def to_dict(self):
            return {"task_id": "partition-task-retry", "status": "running", "result": None}

    class Workflow:
        store = Store()

        def add_task_event_listener(self, listener):
            self.listener = listener

        def get_task(self, task_id):
            assert task_id == "partition-task-retry"
            return CurrentTask()

    class Repository:
        def __init__(self):
            self.updates = []
            self.rebinds = []

        def update_partition_task(self, task_id, status, result=None):
            self.updates.append((task_id, status, result))
            return "partition-run-001" if len(self.updates) > 1 else None

        def rebind_partition_task(self, source_task_id, task_id):
            self.rebinds.append((source_task_id, task_id))
            return "partition-run-001"

    workflow = Workflow()
    repository = Repository()
    SceneDomainService(repository, workflow)

    workflow.listener("partition-task-retry", "running", None)

    assert repository.rebinds == [("partition-task-original", "partition-task-retry")]
    assert repository.updates == [
        ("partition-task-retry", "running", None),
        ("partition-task-retry", "running", None),
    ]


def test_partition_projection_advances_dataset_current_output() -> None:
    statements = []

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params=()):
            statements.append((sql, params))

        def fetchone(self):
            return ("partition-run-001",)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

        def commit(self):
            return None

    repository = OpenGaussSceneRepository(None, connection_factory=Connection)
    repository.update_partition_task(
        "partition-task-001",
        "completed",
        {"datasets": [{"dataset_id": "dataset-optical", "status": "completed", "output_version": "output-v2"}]},
    )

    assert any(
        "UPDATE datasets SET current_output_version" in sql
        and params == ("output-v2", "dataset-optical")
        for sql, params in statements
    )
    run_update = next(sql for sql, _ in statements if "UPDATE partition_runs SET status" in sql)
    assert "CASE status" in run_update
    assert "END < %s OR status = %s\n                      )" in run_update


def test_retry_rebind_preserves_completed_scene_outputs() -> None:
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params=()):
            statements.append((sql, params))

        def fetchone(self):
            return ("partition-run-001",)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

        def commit(self):
            return None

    repository = OpenGaussSceneRepository(None, connection_factory=Connection)

    assert repository.rebind_partition_task("partition-task-old", "partition-task-new") == "partition-run-001"

    scene_update = next(sql for sql, _ in statements if "UPDATE partition_run_scenes SET status='queued'" in sql)
    assert "status <> 'completed'" in scene_update


def test_partition_projection_does_not_regress_terminal_run() -> None:
    statements = []

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params=()):
            statements.append((sql, params))
            if "UPDATE partition_runs SET status" in sql:
                self.rowcount = 0

        def fetchone(self):
            return ("partition-run-001",)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

        def commit(self):
            return None

    repository = OpenGaussSceneRepository(None, connection_factory=Connection)
    repository.update_partition_task("partition-task-new", "queued")

    assert not any("UPDATE partition_run_scenes" in sql for sql, _ in statements)


def test_scene_contract_rejects_legacy_batch_id_and_duplicate_scene() -> None:
    payload = _payload()
    payload["batch_id"] = "load-001"

    with pytest.raises(ValidationError) as exc_info:
        ScenePartitionRunRequest.model_validate(payload)

    errors = exc_info.value.errors()
    assert any(error["type"] == "extra_forbidden" and error["loc"] == ("batch_id",) for error in errors)

    duplicate_payload = _payload()
    duplicate_payload["datasets"][1]["scene_ids"] = ["scene-optical"]
    with pytest.raises(ValidationError, match="selected once"):
        ScenePartitionRunRequest.model_validate(duplicate_payload)


@pytest.mark.parametrize(
    "partition_override",
    [
        {"grid_type": "mgrs", "requested_grid_level": 1, "partition_method": "logical"},
        {"grid_type": "isea4h", "requested_grid_level": 2, "partition_method": "entity"},
    ],
    ids=("different-grid-type", "different-grid-level"),
)
def test_scene_contract_rejects_mixed_grid_configuration_in_one_run(partition_override) -> None:
    payload = _payload()
    payload["datasets"][1]["partition"].update(partition_override)

    with pytest.raises(ValidationError, match="same grid type and level"):
        ScenePartitionRunRequest.model_validate(payload)


def test_execution_request_does_not_copy_source_batch_ids_into_dataset_identity() -> None:
    request = ScenePartitionRunRequest.model_validate(_payload())
    datasets = (
        _dataset("dataset-optical", "optical", "scene-optical"),
        _dataset("dataset-carbon", "carbon", "scene-carbon"),
    )

    adapted = build_partition_execution_request(request, datasets)

    dumped = adapted.model_dump(mode="json")
    assert dumped["batch_id"] == "partition-run-001"
    assert "source_batch_ids" not in dumped
    assert deepcopy(request.source_batch_ids) == ("load-001",)


def test_load_schema_supports_multiple_datasets_and_multiple_assets_per_scene() -> None:
    datasets = _load_schema_datasets(
        {
            "load_batch_id": "load-mixed",
            "datasets": [
                {
                    "dataset_id": "dataset-optical",
                    "data_type": "optical",
                    "scenes": [
                        {
                            "scene_key": "scene-a",
                            "assets": [
                                {
                                    "asset_id": "asset-red",
                                    "source_uri": "s3://cube/source/red.tif",
                                    "checksum": "a" * 64,
                                    "acquisition_time": "2026-07-01T00:00:00Z",
                                    "bbox": [100, 20, 101, 21],
                                    "crs": "EPSG:4326",
                                    "resolution": "10m",
                                    "bands": [{"band_code": "B04", "band_name": "Red"}],
                                },
                                {
                                    "asset_id": "asset-nir",
                                    "source_uri": "s3://cube/source/nir.tif",
                                    "checksum": "b" * 64,
                                    "acquisition_time": "2026-07-01T00:00:00Z",
                                    "bbox": [100, 20, 101, 21],
                                    "crs": "EPSG:4326",
                                    "bands": [{"band_code": "B08", "band_name": "NIR"}],
                                },
                            ],
                        }
                    ],
                },
                {
                    "dataset_id": "dataset-carbon",
                    "data_type": "carbon",
                    "scenes": [
                        {
                            "scene_key": "orbit-1",
                            "source_uri": "s3://cube/source/orbit-1.nc",
                            "observation_id": "orbit-1",
                            "variable": "xco2",
                        }
                    ],
                },
            ],
        },
        load_batch_id="load-mixed",
    )

    assert [dataset["dataset_id"] for dataset in datasets] == ["dataset-optical", "dataset-carbon"]
    assert [asset["asset_id"] for asset in datasets[0]["scenes"][0]["assets"]] == ["asset-red", "asset-nir"]
    assert datasets[0]["scenes"][0]["attributes"]["resolution"] == "10m"
    assert datasets[0]["scenes"][0]["assets"][0]["attributes"]["resolution"] == "10m"
    assert datasets[1]["scenes"][0]["assets"][0]["bands"][0]["band_code"] == "xco2"


def test_load_schema_generates_stable_band_code_at_ingest_boundary() -> None:
    manifest = {
        "source_system": "loader-a",
        "datasets": [
            {
                "dataset_id": "dataset-optical",
                "data_type": "optical",
                "product_type": "surface-reflectance",
                "scenes": [
                    {
                        "scene_key": "scene-a",
                        "source_uri": "s3://cube/source/a.tif",
                        "asset_id": "asset-a",
                        "bands": [{"band_name": "Red"}, {"band_name": "NIR"}],
                    }
                ],
            }
        ],
    }

    first = _load_schema_datasets(manifest, load_batch_id="load-001")[0]["scenes"][0]["assets"][0]["bands"]
    later = _load_schema_datasets(manifest, load_batch_id="load-002")[0]["scenes"][0]["assets"][0]["bands"]
    other_product = deepcopy(manifest)
    other_product["datasets"][0]["product_type"] = "top-of-atmosphere"
    other = _load_schema_datasets(other_product, load_batch_id="load-003")[0]["scenes"][0]["assets"][0]["bands"]

    assert [band["band_code"] for band in first] == [band["band_code"] for band in later]
    assert first[0]["band_code"].startswith("auto-optical-")
    assert first[0]["band_code"] != first[1]["band_code"]
    assert first[0]["band_code"] != other[0]["band_code"]
    assert first[0]["band_type"] == "spectral"
    assert first[0]["attributes"] == {
        "band_code_generated": True,
        "band_code_basis": "data_type+product_type+asset_id+band_index",
        "source_band_index": 1,
    }


def test_load_schema_preserves_source_native_band_code() -> None:
    dataset = _load_schema_datasets(
        {
            "datasets": [
                {
                    "dataset_id": "dataset-radar",
                    "data_type": "radar",
                    "scenes": [
                        {
                            "source_uri": "s3://cube/source/vv.tif",
                            "asset_id": "asset-vv",
                            "bands": [{"code": "VV", "band_name": "Vertical transmit vertical receive"}],
                        }
                    ],
                }
            ]
        },
        load_batch_id="load-radar",
    )[0]

    band = dataset["scenes"][0]["assets"][0]["bands"][0]
    assert band["band_code"] == "VV"
    assert band["band_type"] == "polarization"
    assert band["attributes"] == {"source_band_index": 1}


def test_multi_asset_scene_uses_scene_level_acquisition_time_as_fallback() -> None:
    scene = _load_schema_datasets(
        {"datasets": [{
            "dataset_id": "dataset-optical", "data_type": "optical",
            "scenes": [{
                "scene_key": "scene-a", "acquisition_time": "2024-06-15T00:00:00Z",
                "assets": [{
                    "asset_id": "asset-a", "source_uri": "s3://user-1/cog/a.tif",
                    "bands": [{"band_code": "B04"}],
                }],
            }],
        }]},
        load_batch_id="load-scene-time",
    )[0]["scenes"][0]

    assert scene["acquisition_time"] == "2024-06-15T00:00:00Z"


def test_multi_asset_scene_requires_asset_level_band_mapping() -> None:
    with pytest.raises(ValueError, match="declare bands on each asset"):
        _load_schema_datasets(
            {
                "source_system": "loader-a",
                "datasets": [
                    {
                        "dataset_id": "dataset-a",
                        "data_type": "optical",
                        "scenes": [
                            {
                                "scene_key": "scene-a",
                                "bands": ["B04"],
                                "assets": [{"asset_id": "a", "source_uri": "s3://cube/source/a.tif"}],
                            }
                        ],
                    }
                ],
            },
            load_batch_id="load-001",
        )


def test_load_schema_rejects_data_asset_without_band_units() -> None:
    with pytest.raises(ValueError, match="data asset requires at least one band"):
        _load_schema_datasets(
            {"datasets": [{
                "dataset_id": "dataset-optical",
                "data_type": "optical",
                "scenes": [{"source_uri": "s3://cube/source/a.tif", "asset_id": "asset-a"}],
            }]},
            load_batch_id="load-no-bands",
        )


@pytest.mark.parametrize("indexes, message", [
    ([0], "positive integer"),
    ([1.9], "positive integer"),
    ([True], "positive integer"),
    ([1, 1], "duplicate source_band_index"),
])
def test_load_schema_rejects_invalid_source_band_indexes(indexes, message) -> None:
    bands = [
        {"band_code": f"B{position + 1}", "source_band_index": source_index}
        for position, source_index in enumerate(indexes)
    ]
    with pytest.raises(ValueError, match=message):
        _load_schema_datasets(
            {"datasets": [{
                "dataset_id": "dataset-optical",
                "data_type": "optical",
                "scenes": [{"source_uri": "s3://cube/source/a.tif", "asset_id": "asset-a", "bands": bands}],
            }]},
            load_batch_id="load-invalid-indexes",
        )


def test_multi_dataset_schema_import_uses_formal_scene_domain() -> None:
    class SceneService:
        def import_load_schema(self, payload):
            assert len(payload["datasets"]) == 2
            return {"load_batch_id": payload["load_batch_id"], "status": "succeeded", "dataset_count": 2, "scene_count": 2}

    payload = {"load_batch_id": "load-mixed", "datasets": [{"dataset_id": "a"}, {"dataset_id": "b"}]}

    result = import_partition_schema_payload(object(), SceneService(), payload)

    assert result["status"] == "imported"
    assert result["load_batch_id"] == "load-mixed"
    assert result["dataset_count"] == 2


def test_public_import_contract_accepts_explicit_load_batch_id() -> None:
    payload = PartitionSchemaImportRequest.model_validate(
        {
            "load_batch_id": "load-mixed",
            "datasets": [{"dataset_id": "dataset-a", "scenes": [{"scene_key": "scene-a"}]}],
        }
    )

    assert payload.load_batch_id == "load-mixed"


def test_scene_import_is_additive_and_writes_scene_assets_and_bands() -> None:
    class Cursor:
        def __init__(self):
            self.statements = []
            self.last_sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params=()):
            self.last_sql = " ".join(sql.split())
            self.statements.append((self.last_sql, params))

        def fetchone(self):
            if self.last_sql.startswith("SELECT scene_id,dataset_id FROM scenes"):
                return ("persisted-scene", "dataset-a")
            return None

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.committed = False

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.committed = True

        def close(self):
            return None

    connection = Connection()
    repository = OpenGaussSceneRepository(None, connection_factory=lambda: connection)

    result = repository.upsert_load_schema(
        {
            "load_batch_id": "load-001",
            "datasets": [
                {
                    "dataset_id": "dataset-a",
                    "data_type": "optical",
                    "scenes": [
                        {
                            "scene_key": "scene-a",
                            "source_uri": "s3://cube/source/a.tif",
                            "asset_id": "asset-a",
                            "checksum": "a" * 64,
                            "acquisition_time": "2026-07-01T00:00:00Z",
                            "bbox": [100, 20, 101, 21],
                            "crs": "EPSG:4326",
                            "bands": [{"band_name": "Red"}],
                        }
                    ],
                }
            ],
        }
    )

    sql = "\n".join(statement for statement, _ in connection.cursor_instance.statements)
    assert result == {"load_batch_id": "load-001", "status": "succeeded", "dataset_count": 1, "scene_count": 1}
    assert connection.committed is True
    assert "MERGE INTO load_batches" in sql
    assert "MERGE INTO scenes" in sql
    assert "MERGE INTO scene_assets" in sql
    assert "MERGE INTO scene_bands" in sql
    assert "MERGE INTO load_batch_scenes" in sql
    assert "DELETE FROM" not in sql
    assert "DROP TABLE" not in sql
    band_write = next(
        params
        for statement, params in connection.cursor_instance.statements
        if statement.startswith("MERGE INTO scene_bands")
    )
    assert band_write[2].startswith("band-")
    assert band_write[3].startswith("auto-optical-")
    assert band_write[4:6] == ("Red", "spectral")
    assert json.loads(band_write[8])["band_code_generated"] is True
    assert json.loads(band_write[8])["source_band_index"] == 1


def test_partition_scene_idempotency_is_scoped_to_run() -> None:
    config = {"grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity"}

    first = _partition_scene_idempotency_key("run-a", "scene-a", config)
    retry = _partition_scene_idempotency_key("run-b", "scene-a", config)

    assert first != retry
    assert first == _partition_scene_idempotency_key("run-a", "scene-a", config)


def test_opengauss_load_batch_scenes_include_ordered_band_metadata() -> None:
    repository = OpenGaussSceneRepository(None)

    def read(sql, _params):
        if "FROM load_batch_scenes" in sql:
            return [{
                "scene_id": "scene-a", "dataset_id": "dataset-a", "dataset_code": "DS-A",
                "dataset_title": "Optical A", "data_type": "optical", "product_type": "L2A",
                "load_batch_id": "load-a", "load_status": "succeeded",
                "attributes": {"resolution": "10m"},
            }]
        if "FROM scene_bands" in sql:
            return [
                {"scene_id": "scene-a", "asset_id": "asset-a", "band_unit_id": "band-a-b04", "band_code": "B04", "band_name": "红光", "band_type": "spectral", "unit": None, "display_order": 1, "attributes": {}},
                {"scene_id": "scene-a", "asset_id": "asset-a", "band_unit_id": "band-a-b08", "band_code": "B08", "band_name": "近红外", "band_type": "spectral", "unit": None, "display_order": 2, "attributes": {}},
            ]
        if "FROM partition_data_unit_grid_status" in sql:
            return []
        raise AssertionError(sql)

    repository._read = read
    rows = repository.list_load_batch_scenes("load-a")

    assert [band["band_code"] for band in rows[0]["bands"]] == ["B04", "B08"]
    assert [band["band_unit_id"] for band in rows[0]["bands"]] == ["band-a-b04", "band-a-b08"]


def test_load_batch_partition_rejects_a_band_already_partitioned_and_ingested() -> None:
    repository = OpenGaussSceneRepository(None)

    def read(sql, _params):
        if "FROM load_batches" in sql:
            return [{"load_batch_id": "load-a"}]
        if "FROM scenes s" in sql:
            return [{
                "scene_id": "scene-a", "dataset_id": "dataset-a", "dataset_code": "DS-A",
                "dataset_title": "Optical A", "data_type": "optical", "product_type": "L2A",
                "dataset_attributes": {}, "load_batch_id": "load-a",
            }]
        if "FROM scene_assets" in sql:
            return []
        if "SELECT b.* FROM scene_bands" in sql:
            return [{"scene_id": "scene-a", "asset_id": "asset-a", "band_unit_id": "band-a-b04"}]
        if "SELECT DISTINCT g.band_unit_id,g.grid_type,g.grid_level" in sql:
            return [{"band_unit_id": "band-a-b04", "grid_type": "mgrs", "grid_level": 1}]
        raise AssertionError(sql)

    repository._read = read
    request = ScenePartitionRunRequest.model_validate({
        "partition_run_id": "partition-run-a",
        "source_batch_ids": ["load-a"],
        "selection_source": "load_batch",
        "datasets": [{
            "dataset_id": "dataset-a", "scene_ids": ["scene-a"], "band_unit_ids": ["band-a-b04"],
            "partition": {"grid_type": "geohash", "requested_grid_level": 4, "partition_method": "logical"},
        }],
    })

    with pytest.raises(ValueError, match="already partitioned and ingested"):
        repository.materialize_partition_datasets(request)


def test_load_batch_scene_groups_include_resolution_grid_recommendations() -> None:
    repository = _Repository()
    repository.list_load_batch_scenes = lambda *_args, **_kwargs: [{
        "scene_id": "scene-optical", "dataset_id": "dataset-optical",
        "dataset_code": "OPTICAL", "dataset_title": "Optical", "data_type": "optical",
        "attributes": {"resolution": "10m"},
    }]
    service = SceneDomainService(repository, _Workflow())

    result = service.list_load_batch_scenes("load-001")

    assert result["datasets"][0]["resolution_m"] == 10
    assert result["datasets"][0]["suggested_grid_levels"] == {"geohash": 5, "mgrs": 0, "isea4h": 11}


def test_geographic_scene_resolution_keeps_degrees_and_recommends_geohash() -> None:
    repository = _Repository()
    repository.list_load_batch_scenes = lambda *_args, **_kwargs: [{
        "scene_id": "scene-optical", "dataset_id": "dataset-optical",
        "dataset_code": "OPTICAL", "dataset_title": "Optical", "data_type": "optical",
        "crs": "EPSG:4326", "attributes": {"resolution_m": 0.00030906354339487385},
    }]
    service = SceneDomainService(repository, _Workflow())

    dataset = service.list_load_batch_scenes("load-001")["datasets"][0]

    assert dataset["crs"] == "EPSG:4326"
    assert dataset["resolution_native"] == pytest.approx(0.00030906354339487385)
    assert dataset["resolution_unit"] == "degree"
    assert dataset["resolution_m"] == pytest.approx(34.403, rel=1e-3)
    assert dataset["suggested_grid_type"] == "geohash"
    assert dataset["suggested_grid_levels"] == {"geohash": 4, "mgrs": 0, "isea4h": 8}


def test_projected_scene_resolution_keeps_meters_and_recommends_mgrs() -> None:
    repository = _Repository()
    repository.list_load_batch_scenes = lambda *_args, **_kwargs: [{
        "scene_id": "scene-radar", "dataset_id": "dataset-radar",
        "dataset_code": "RADAR", "dataset_title": "Radar", "data_type": "radar",
        "crs": "EPSG:32650", "attributes": {"resolution_m": 10},
    }]
    service = SceneDomainService(repository, _Workflow())

    dataset = service.list_load_batch_scenes("load-001")["datasets"][0]

    assert dataset["resolution_native"] == 10
    assert dataset["resolution_unit"] == "m"
    assert dataset["resolution_m"] == 10
    assert dataset["suggested_grid_type"] == "mgrs"
    assert dataset["suggested_grid_levels"] == {"geohash": 5, "mgrs": 0, "isea4h": 11}
