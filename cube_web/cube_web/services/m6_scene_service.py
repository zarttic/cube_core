from __future__ import annotations

from typing import Any, Protocol

from cube_web.services.http_errors import HTTPException
from cube_web.services.m6_scene_contracts import ScenePartitionRunRequest
from cube_web.services.partition_contracts import DatasetInput, StrictPartitionRequest
from cube_web.services.partition_defaults import default_grid_level_for_resolution, finest_resolution_from_assets


class SceneRepository(Protocol):
    def upsert_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any] | None: ...

    def list_load_batch_scenes(
        self,
        load_batch_id: str,
        *,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def materialize_partition_datasets(self, request: ScenePartitionRunRequest) -> tuple[DatasetInput, ...]: ...

    def create_partition_run(self, request: ScenePartitionRunRequest) -> dict[str, Any]: ...

    def bind_partition_task(self, partition_run_id: str, task_id: str) -> None: ...

    def fail_partition_run(self, partition_run_id: str, error_message: str) -> None: ...

    def update_partition_task(self, task_id: str, status: str, result: dict[str, Any] | None = None) -> None: ...


class SceneDomainService:
    def __init__(self, repository: SceneRepository, workflow: Any) -> None:
        self.repository = repository
        self.workflow = workflow
        add_listener = getattr(workflow, "add_task_event_listener", None)
        if add_listener is not None:
            add_listener(self._on_partition_task_event)

    def _on_partition_task_event(self, task_id: str, status: str, result: dict[str, Any] | None) -> None:
        self.repository.update_partition_task(task_id, status, result)

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return {
            "load_batches": self.repository.list_load_batches(
                status=status,
                data_type=data_type,
                keyword=keyword,
                limit=limit,
            )
        }

    def import_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.repository.upsert_load_schema(payload)

    def should_sync_import(self, workflow_store: Any) -> bool:
        # A workflow using its isolated in-memory scheduler must not write its
        # fixture data into the separately configured production OpenGauss DB.
        from cube_web.services.partition_job_store import InMemoryPartitionJobStore

        return not isinstance(workflow_store, InMemoryPartitionJobStore)

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any]:
        batch = self.repository.get_load_batch(load_batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"Load batch not found: {load_batch_id}")
        return batch

    def list_load_batch_scenes(
        self,
        load_batch_id: str,
        *,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> dict[str, Any]:
        batch = self.get_load_batch(load_batch_id)
        scenes = self.repository.list_load_batch_scenes(
            load_batch_id,
            status=status,
            data_type=data_type,
            dataset_id=dataset_id,
        )
        grouped: dict[str, dict[str, Any]] = {}
        for scene in scenes:
            group_id = str(scene["dataset_id"])
            group = grouped.setdefault(
                group_id,
                {
                    "dataset_id": group_id,
                    "dataset_code": scene.get("dataset_code"),
                    "dataset_title": scene.get("dataset_title"),
                    "data_type": scene.get("data_type"),
                    "product_type": scene.get("product_type"),
                    "scenes": [],
                },
            )
            group["scenes"].append(scene)
        for group in grouped.values():
            if group.get("data_type") == "carbon":
                continue
            resolution = finest_resolution_from_assets(group["scenes"])
            if resolution is None:
                continue
            group["resolution_m"] = resolution
            group["suggested_grid_levels"] = {
                grid_type: default_grid_level_for_resolution(resolution, grid_type=grid_type)
                for grid_type in ("geohash", "mgrs", "isea4h")
            }
        return {
            "load_batch": batch,
            "datasets": list(grouped.values()),
            "scene_count": len(scenes),
        }

    def submit_partition_run(self, request: ScenePartitionRunRequest) -> dict[str, Any]:
        datasets = self.repository.materialize_partition_datasets(request)
        strict_request = adapt_scene_partition_request(request, datasets)
        run = self.repository.create_partition_run(request)
        if not run.get("created"):
            attributes = run.get("attributes") if isinstance(run.get("attributes"), dict) else {}
            task_id = str(attributes.get("task_id") or "")
            if task_id:
                data_types = {dataset.data_type for dataset in datasets}
                return {
                    "partition_run_id": request.partition_run_id,
                    "source_batch_ids": list(request.source_batch_ids),
                    "task_id": task_id,
                    "status": str(run.get("status") or "queued"),
                    "data_type": "mixed" if len(data_types) > 1 else next(iter(data_types)),
                    "operation": "run",
                }
            raise HTTPException(
                status_code=409,
                detail=f"Partition run is being created: {request.partition_run_id}",
            )
        data_types = {dataset.data_type for dataset in datasets}
        try:
            if len(data_types) > 1:
                task = self.workflow.submit_mixed(strict_request)
            else:
                task = self.workflow.submit_strict(next(iter(data_types)), strict_request)
            self.repository.bind_partition_task(request.partition_run_id, task.task_id)
        except Exception as exc:
            self.repository.fail_partition_run(request.partition_run_id, str(exc))
            raise
        try:
            current = self.workflow.get_task(task.task_id).to_dict()
        except Exception:
            current = None
        if current is not None and str(current.get("status") or "") != "queued":
            self.repository.update_partition_task(
                task.task_id,
                str(current.get("status") or "queued"),
                current.get("result") if isinstance(current.get("result"), dict) else None,
            )
        task_value = task.to_dict()
        return {
            "partition_run_id": request.partition_run_id,
            "source_batch_ids": list(request.source_batch_ids),
            "task_id": task_value["task_id"],
            "status": task_value["status"],
            "data_type": task_value["data_type"],
            "operation": task_value["operation"],
        }


def adapt_scene_partition_request(
    request: ScenePartitionRunRequest,
    datasets: tuple[DatasetInput, ...],
) -> StrictPartitionRequest:
    """Explicit compatibility boundary for the M2 executor's legacy batch key.

    The executor still stores its queue identity in ``batch_id``. Only this
    adapter maps the M6 ``partition_run_id`` to that field; load batch IDs are
    retained separately on the M6 run and never become execution IDs.
    """
    if not datasets:
        raise ValueError("partition run must resolve at least one dataset")
    selections = {selection.dataset_id: selection for selection in request.datasets}
    resolved: list[DatasetInput] = []
    for dataset in datasets:
        selection = selections.get(dataset.dataset_id)
        if selection is None:
            raise ValueError(f"resolved unexpected dataset: {dataset.dataset_id}")
        resolved.append(dataset.model_copy(update={"partition": selection.partition}))
    first = request.datasets[0].partition
    missing = [
        name
        for name in ("grid_type", "requested_grid_level", "partition_method")
        if getattr(first, name) is None
    ]
    if missing:
        raise ValueError(f"dataset partition requires explicit fields: {', '.join(missing)}")
    return StrictPartitionRequest(
        batch_id=request.partition_run_id,
        grid_type=first.grid_type,
        requested_grid_level=first.requested_grid_level,
        partition_method=first.partition_method,
        cover_mode=first.cover_mode or "intersect",
        time_granularity=first.time_granularity or "day",
        max_cells_per_asset=first.max_cells_per_asset or 0,
        datasets=tuple(resolved),
    )
