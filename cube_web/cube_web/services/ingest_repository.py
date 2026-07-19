from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from threading import RLock
from typing import Any, Protocol
from uuid import uuid4

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cube_web.services.db_pool import _PostgresPool
from cube_web.services.ingest_contracts import CreateIngestRun, IngestRun, IngestRunScene, IngestSummary


class IngestRunNotFound(LookupError):
    pass


class IngestSceneNotFound(LookupError):
    pass


class IngestConflict(RuntimeError):
    pass


class InvalidIngestTransition(RuntimeError):
    pass


def scene_idempotency_key(
    dataset_id: str,
    scene_id: str,
    output_version: str,
    band_unit_ids: tuple[str, ...] = (),
) -> str:
    value = f"{dataset_id}\0{scene_id}\0{output_version}\0{','.join(sorted(set(band_unit_ids)))}".encode()
    return hashlib.sha256(value).hexdigest()


class IngestRepository(Protocol):
    def create(self, ingest_run_id: str, request: CreateIngestRun) -> IngestRun: ...

    def get(self, ingest_run_id: str) -> IngestRun: ...

    def list_runs(
        self,
        *,
        keyword: str | None,
        dataset_id: str | None,
        status: str | None,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: str,
    ) -> tuple[IngestRun, ...]: ...

    def count_runs(self, *, keyword: str | None, dataset_id: str | None, status: str | None) -> int: ...

    def summarize_runs(self, *, keyword: str | None, dataset_id: str | None, status: str | None) -> IngestSummary: ...

    def start_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun: ...

    def complete_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun: ...

    def fail_scene(self, ingest_run_id: str, scene_id: str, error_message: str) -> IngestRun: ...

    def retry_failed(self, ingest_run_id: str, band_unit_ids: tuple[str, ...] | None = None, *, requested_by: str = "system") -> IngestRun: ...

    def cancel(self, ingest_run_id: str, reason: str = "") -> IngestRun: ...

    def claim_queued_outputs(self, *, limit: int = 10) -> tuple[dict[str, Any], ...]: ...


def _now() -> datetime:
    return datetime.now(UTC)


def _aggregate_status(scene_statuses: list[str]) -> str:
    if scene_statuses and all(status == "completed" for status in scene_statuses):
        return "completed"
    if scene_statuses and all(status == "failed" for status in scene_statuses):
        return "failed"
    if any(status == "completed" for status in scene_statuses) and any(status in {"failed", "cancelled"} for status in scene_statuses):
        return "partial_failure"
    if any(status == "running" for status in scene_statuses):
        return "running"
    if any(status in {"pending", "queued"} for status in scene_statuses):
        return "queued"
    if scene_statuses and all(status == "cancelled" for status in scene_statuses):
        return "cancelled"
    return "pending"


class InMemoryIngestRepository:
    def __init__(self, scene_datasets: dict[str, str]) -> None:
        self.scene_datasets = dict(scene_datasets)
        self.scene_statuses = {scene_id: "quality_passed" for scene_id in scene_datasets}
        self.runs: dict[str, dict[str, Any]] = {}
        self.idempotency: dict[str, tuple[str, str]] = {}
        self._lock = RLock()

    def create(self, ingest_run_id: str, request: CreateIngestRun) -> IngestRun:
        with self._lock:
            existing = self.runs.get(ingest_run_id)
            if existing is not None:
                if self._request_identity(existing) != self._input_identity(request):
                    raise IngestConflict(f"ingest run {ingest_run_id} already exists with a different request")
                return self._model(existing)
            if len({scene.scene_id for scene in request.scenes}) != len(request.scenes):
                raise IngestConflict("an ingest run cannot contain the same scene more than once")
            owners = {
                self.idempotency[key][0]
                for scene in request.scenes
                if (key := scene_idempotency_key(request.dataset_id, scene.scene_id, scene.output_version, scene.band_unit_ids)) in self.idempotency
            }
            if owners:
                if len(owners) == 1:
                    existing_run = self.runs[next(iter(owners))]
                    if self._request_outputs(existing_run) == self._input_outputs(request):
                        return self._model(existing_run)
                raise IngestConflict("one or more scene outputs already belong to a different ingest request")
            now = _now()
            scene_rows: dict[str, dict[str, Any]] = {}
            for scene in request.scenes:
                if self.scene_datasets.get(scene.scene_id) != request.dataset_id:
                    raise IngestConflict(f"scene {scene.scene_id} does not belong to dataset {request.dataset_id}")
                key = scene_idempotency_key(request.dataset_id, scene.scene_id, scene.output_version, scene.band_unit_ids)
                owner = self.idempotency.get(key)
                if owner is not None:
                    raise IngestConflict(f"scene output is already owned by ingest run {owner[0]}")
                scene_rows[scene.scene_id] = {
                    "ingest_run_id": ingest_run_id,
                    "scene_id": scene.scene_id,
                    "partition_run_id": request.partition_run_id,
                    "output_version": scene.output_version,
                    "status": "queued",
                    "idempotency_key": key,
                    "attempt_count": 0,
                    "error_message": None,
                    "quality_run_id": scene.quality_run_id,
                    "source_load_batch_ids": tuple(scene.source_load_batch_ids),
                    "band_unit_ids": tuple(scene.band_unit_ids),
                    "retry_history": [],
                    "created_at": now,
                    "updated_at": now,
                }
            run = {
                "ingest_run_id": ingest_run_id,
                "partition_run_id": request.partition_run_id,
                "dataset_id": request.dataset_id,
                "dataset_code": request.dataset_id,
                "status": "queued",
                "requested_by": request.requested_by,
                "error_message": None,
                "created_at": now,
                "started_at": None,
                "completed_at": None,
                "cancel_reason": None,
                "scenes": scene_rows,
            }
            self.runs[ingest_run_id] = run
            for row in scene_rows.values():
                self.idempotency[row["idempotency_key"]] = (ingest_run_id, row["scene_id"])
            return self._model(run)

    def get(self, ingest_run_id: str) -> IngestRun:
        with self._lock:
            return self._model(self._run(ingest_run_id))

    def list_runs(
        self,
        *,
        keyword: str | None = None,
        dataset_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> tuple[IngestRun, ...]:
        _validate_page(limit, offset)
        _validate_sort(sort_by, sort_order)
        with self._lock:
            rows = self._filtered(keyword=keyword, dataset_id=dataset_id, status=status)
            rows.sort(key=lambda item: (item[sort_by], item["ingest_run_id"]), reverse=sort_order.lower() == "desc")
            return tuple(self._model(run) for run in rows[offset : offset + limit])

    def count_runs(self, *, keyword: str | None = None, dataset_id: str | None = None, status: str | None = None) -> int:
        with self._lock:
            return len(self._filtered(keyword=keyword, dataset_id=dataset_id, status=status))

    def summarize_runs(self, *, keyword: str | None = None, dataset_id: str | None = None, status: str | None = None) -> IngestSummary:
        with self._lock:
            runs = self._filtered(keyword=keyword, dataset_id=dataset_id, status=status)
            scenes = [scene for run in runs for scene in run["scenes"].values()]
            return IngestSummary(
                run_count=len(runs),
                scene_count=len(scenes),
                completed_scene_count=sum(scene["status"] == "completed" for scene in scenes),
                failed_scene_count=sum(scene["status"] == "failed" for scene in scenes),
            )

    def start_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        with self._lock:
            run, scene = self._scene(ingest_run_id, scene_id)
            if scene["status"] == "completed":
                return self._model(run)
            if scene["status"] not in {"pending", "queued"}:
                raise InvalidIngestTransition(f"cannot start scene in status {scene['status']}")
            now = _now()
            scene.update(status="running", attempt_count=scene["attempt_count"] + 1, error_message=None, updated_at=now)
            self.scene_statuses[scene_id] = "ingesting"
            run["started_at"] = run["started_at"] or now
            self._refresh(run, now)
            return self._model(run)

    def complete_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        return self._finish_scene(ingest_run_id, scene_id, status="completed", error_message=None)

    def fail_scene(self, ingest_run_id: str, scene_id: str, error_message: str) -> IngestRun:
        if not error_message.strip():
            raise ValueError("error_message must not be empty")
        return self._finish_scene(ingest_run_id, scene_id, status="failed", error_message=error_message)

    def retry_failed(self, ingest_run_id: str, band_unit_ids: tuple[str, ...] | None = None, *, requested_by: str = "system") -> IngestRun:
        with self._lock:
            run = self._run(ingest_run_id)
            selected = set(band_unit_ids) if band_unit_ids is not None else None
            known = {band for scene in run["scenes"].values() for band in scene["band_unit_ids"]}
            if selected is not None and (not selected or selected - known):
                raise IngestSceneNotFound(",".join(sorted(selected - known)))
            failed = [
                scene
                for scene in run["scenes"].values()
                if scene["status"] == "failed" and (selected is None or any(band in selected for band in scene["band_unit_ids"]))
            ]
            if selected is not None and len(failed) != len(selected):
                raise InvalidIngestTransition("only failed band units can be retried")
            if not failed:
                raise InvalidIngestTransition("ingest run has no failed band units to retry")
            now = _now()
            for scene in failed:
                scene["retry_history"].append(
                    {
                        "error_message": scene["error_message"],
                        "retried_by": requested_by,
                        "retried_at": now,
                        "attempt_count": scene["attempt_count"],
                    }
                )
                scene.update(status="queued", error_message=None, updated_at=now)
            run.update(status="queued", error_message=None, completed_at=None)
            return self._model(run)

    def cancel(self, ingest_run_id: str, reason: str = "") -> IngestRun:
        with self._lock:
            run = self._run(ingest_run_id)
            if run["status"] in {"completed", "partial_failure", "failed", "cancelled"}:
                raise InvalidIngestTransition(f"cannot cancel ingest run in status {run['status']}")
            now = _now()
            for scene in run["scenes"].values():
                if scene["status"] in {"pending", "queued", "running"}:
                    scene.update(status="cancelled", updated_at=now)
            run.update(status="cancelled", completed_at=now, cancel_reason=reason or None)
            return self._model(run)

    def _finish_scene(self, ingest_run_id: str, scene_id: str, *, status: str, error_message: str | None) -> IngestRun:
        with self._lock:
            run, scene = self._scene(ingest_run_id, scene_id)
            if scene["status"] == status and status == "completed":
                return self._model(run)
            if scene["status"] != "running":
                raise InvalidIngestTransition(f"cannot finish scene in status {scene['status']}")
            now = _now()
            scene.update(status=status, error_message=error_message, updated_at=now)
            self.scene_statuses[scene_id] = "available" if status == "completed" else "failed"
            self._refresh(run, now)
            return self._model(run)

    def _refresh(self, run: dict[str, Any], now: datetime) -> None:
        run["status"] = _aggregate_status([scene["status"] for scene in run["scenes"].values()])
        errors = [scene["error_message"] for scene in run["scenes"].values() if scene["error_message"]]
        run["error_message"] = errors[0] if errors and run["status"] in {"failed", "partial_failure"} else None
        run["completed_at"] = now if run["status"] in {"completed", "partial_failure", "failed", "cancelled"} else None

    def _run(self, ingest_run_id: str) -> dict[str, Any]:
        try:
            return self.runs[ingest_run_id]
        except KeyError as exc:
            raise IngestRunNotFound(ingest_run_id) from exc

    def _filtered(self, *, keyword: str | None, dataset_id: str | None, status: str | None) -> list[dict[str, Any]]:
        term = (keyword or "").strip().lower()
        return [
            run
            for run in self.runs.values()
            if (dataset_id is None or run["dataset_id"] == dataset_id)
            and (status is None or run["status"] == status)
            and (not term or term in run["ingest_run_id"].lower() or term in run["dataset_id"].lower())
        ]

    def _scene(self, ingest_run_id: str, scene_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        run = self._run(ingest_run_id)
        try:
            return run, run["scenes"][scene_id]
        except KeyError as exc:
            raise IngestSceneNotFound(scene_id) from exc

    @staticmethod
    def _request_identity(run: dict[str, Any]) -> tuple[Any, ...]:
        scenes = tuple(
            sorted(
                (row["scene_id"], row["output_version"], row["quality_run_id"], row["source_load_batch_ids"], row["band_unit_ids"])
                for row in run["scenes"].values()
            )
        )
        return run["partition_run_id"], run["dataset_id"], run["requested_by"], scenes

    @staticmethod
    def _input_identity(request: CreateIngestRun) -> tuple[Any, ...]:
        scenes = tuple(
            sorted((scene.scene_id, scene.output_version, scene.quality_run_id, scene.source_load_batch_ids, scene.band_unit_ids) for scene in request.scenes)
        )
        return request.partition_run_id, request.dataset_id, request.requested_by, scenes

    @staticmethod
    def _request_outputs(run: dict[str, Any]) -> tuple[Any, ...]:
        scenes = tuple(
            sorted(
                (row["scene_id"], row["output_version"], row["quality_run_id"], row["source_load_batch_ids"], row["band_unit_ids"])
                for row in run["scenes"].values()
            )
        )
        return run["partition_run_id"], run["dataset_id"], scenes

    @staticmethod
    def _input_outputs(request: CreateIngestRun) -> tuple[Any, ...]:
        scenes = tuple(
            sorted((scene.scene_id, scene.output_version, scene.quality_run_id, scene.source_load_batch_ids, scene.band_unit_ids) for scene in request.scenes)
        )
        return request.partition_run_id, request.dataset_id, scenes

    @staticmethod
    def _model(run: dict[str, Any]) -> IngestRun:
        value = copy.deepcopy(run)
        scenes = tuple(IngestRunScene.model_validate(row) for row in value.pop("scenes").values())
        return IngestRun.model_validate({**value, "scenes": scenes})


class OpenGaussIngestRepository:
    """Transactional repository for the managed ingest tables."""

    def __init__(self, dsn: str) -> None:
        self.pool = _PostgresPool.for_dsn(dsn)

    def create(self, ingest_run_id: str, request: CreateIngestRun) -> IngestRun:
        existing = self._existing_request(request)
        if existing is not None:
            return existing
        try:
            with self.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT scene_id,dataset_id FROM scenes WHERE scene_id = ANY(%s)",
                    ([scene.scene_id for scene in request.scenes],),
                )
                owners = {row["scene_id"]: row["dataset_id"] for row in cur.fetchall()}
                invalid = [scene.scene_id for scene in request.scenes if owners.get(scene.scene_id) != request.dataset_id]
                if invalid:
                    raise IngestConflict(f"scenes do not belong to dataset {request.dataset_id}: {sorted(invalid)}")
                cur.execute(
                    "INSERT INTO ingest_runs (ingest_run_id,partition_run_id,dataset_id,status,requested_by) VALUES (%s,%s,%s,'queued',%s) ON CONFLICT (ingest_run_id) DO NOTHING",
                    (ingest_run_id, request.partition_run_id, request.dataset_id, request.requested_by),
                )
                if cur.rowcount != 1:
                    raise IngestConflict(f"ingest run {ingest_run_id} already exists")
                for scene in request.scenes:
                    provenance = {"quality_run_id": scene.quality_run_id, "source_load_batch_ids": list(scene.source_load_batch_ids)}
                    cur.execute(
                        "INSERT INTO ingest_run_scenes (ingest_run_id,scene_id,partition_run_id,output_version,band_unit_ids,status,idempotency_key,provenance) VALUES (%s,%s,%s,%s,%s,'queued',%s,%s)",
                        (
                            ingest_run_id,
                            scene.scene_id,
                            request.partition_run_id,
                            scene.output_version,
                            Jsonb(list(scene.band_unit_ids)),
                            scene_idempotency_key(request.dataset_id, scene.scene_id, scene.output_version, scene.band_unit_ids),
                            Jsonb(provenance),
                        ),
                    )
        except UniqueViolation as exc:
            existing = self._existing_request(request)
            if existing is not None:
                return existing
            raise IngestConflict("one or more scene outputs already have an ingest owner") from exc
        return self.get(ingest_run_id)

    def list_collections(self, *, limit: int = 100, offset: int = 0) -> tuple[dict[str, Any], ...]:
        with self.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT pr.partition_run_id, pr.created_at AS partition_created_at,
                       prs.scene_id, s.scene_key, prs.dataset_id, prs.output_version, g.band_unit_id,
                       d.dataset_code, d.dataset_title, d.data_type,
                       sb.band_code, sb.band_name, sb.band_type, sb.unit, sb.display_order,
                       g.quality_status,
                       irs.status AS ingest_status
                FROM partition_runs pr
                JOIN partition_run_scenes prs ON prs.partition_run_id=pr.partition_run_id AND prs.status='completed'
                JOIN partition_data_unit_grid_status g ON g.partition_run_id=prs.partition_run_id AND g.scene_id=prs.scene_id
                JOIN datasets d ON d.dataset_id=prs.dataset_id
                JOIN scenes s ON s.scene_id=prs.scene_id
                JOIN scene_bands sb ON sb.band_unit_id=g.band_unit_id
                LEFT JOIN LATERAL (
                  SELECT irs.status FROM ingest_run_scenes irs
                  WHERE irs.scene_id=prs.scene_id AND irs.output_version=prs.output_version
                    AND (EXISTS (
                           SELECT 1 FROM jsonb_array_elements_text(irs.band_unit_ids) AS selected(band_unit_id)
                           WHERE selected.band_unit_id=g.band_unit_id
                         ) OR jsonb_array_length(irs.band_unit_ids)=0)
                  ORDER BY irs.updated_at DESC LIMIT 1
                ) irs ON true
                WHERE pr.status='completed' AND g.partition_status='completed'
                ORDER BY pr.created_at DESC, pr.partition_run_id, prs.dataset_id, prs.scene_id, g.band_unit_id
                """,
            )
            rows = cur.fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            collection = grouped.setdefault(str(row["partition_run_id"]), {
                "partition_run_id": str(row["partition_run_id"]),
                "created_at": row["partition_created_at"], "units": [],
            })
            collection["units"].append({
                "scene_id": str(row["scene_id"]), "scene_key": row["scene_key"],
                "dataset_id": str(row["dataset_id"]), "dataset_code": row["dataset_code"],
                "dataset_title": row["dataset_title"], "data_type": row["data_type"],
                "output_version": row["output_version"], "band_unit_id": str(row["band_unit_id"]),
                "band_code": row["band_code"], "band_name": row["band_name"],
                "band_type": row["band_type"], "unit": row["unit"],
                "display_order": row["display_order"],
                "quality_status": row["quality_status"],
                "ingest_status": row["ingest_status"],
            })
        collections = []
        for collection in list(grouped.values())[offset:offset + limit]:
            units = collection["units"]
            collection.update(
                dataset_count=len({unit["dataset_id"] for unit in units}),
                scene_count=len({unit["scene_id"] for unit in units}),
                band_count=len(units),
                quality_pass_count=sum(unit["quality_status"] in {"pass", "warn"} for unit in units),
                ingested_count=sum(unit["ingest_status"] == "completed" for unit in units),
            )
            collection["status"] = "completed" if collection["ingested_count"] == collection["band_count"] else (
                "partial" if collection["ingested_count"] else "pending"
            )
            collections.append(collection)
        return tuple(collections)

    def count_collections(self) -> int:
        with self.pool.connection() as connection, connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM partition_runs WHERE status='completed'")
            return int(cur.fetchone()[0])

    def _existing_request(self, request: CreateIngestRun) -> IngestRun | None:
        keys = [scene_idempotency_key(request.dataset_id, scene.scene_id, scene.output_version, scene.band_unit_ids) for scene in request.scenes]
        with self.pool.connection() as connection, connection.cursor() as cur:
            cur.execute("SELECT DISTINCT ingest_run_id FROM ingest_run_scenes WHERE idempotency_key=ANY(%s)", (keys,))
            owners = [row[0] for row in cur.fetchall()]
        if not owners:
            return None
        if len(owners) != 1:
            raise IngestConflict("scene outputs belong to multiple ingest runs")
        existing = self.get(owners[0])
        expected = (
            request.partition_run_id,
            request.dataset_id,
            tuple(
                sorted(
                    (scene.scene_id, scene.output_version, scene.quality_run_id, scene.source_load_batch_ids, scene.band_unit_ids) for scene in request.scenes
                )
            ),
        )
        actual = (
            existing.partition_run_id,
            existing.dataset_id,
            tuple(
                sorted(
                    (scene.scene_id, scene.output_version, scene.quality_run_id, scene.source_load_batch_ids, scene.band_unit_ids) for scene in existing.scenes
                )
            ),
        )
        if actual != expected:
            raise IngestConflict("one or more scene outputs already belong to a different ingest request")
        return existing

    def get(self, ingest_run_id: str) -> IngestRun:
        with self.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT i.*,d.dataset_code FROM ingest_runs i JOIN datasets d ON d.dataset_id=i.dataset_id WHERE i.ingest_run_id=%s",
                (ingest_run_id,),
            )
            run = cur.fetchone()
            if run is None:
                raise IngestRunNotFound(ingest_run_id)
            cur.execute("SELECT * FROM ingest_run_scenes WHERE ingest_run_id=%s ORDER BY created_at,scene_id", (ingest_run_id,))
            scenes = cur.fetchall()
        return self._model(run, scenes)

    def list_runs(
        self,
        *,
        keyword: str | None = None,
        dataset_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> tuple[IngestRun, ...]:
        _validate_page(limit, offset)
        sort_column, order = _validate_sort(sort_by, sort_order)
        where, params = self._filters(keyword=keyword, dataset_id=dataset_id, status=status)
        with self.pool.connection() as connection, connection.cursor() as cur:
            cur.execute(
                f"SELECT i.ingest_run_id FROM ingest_runs i JOIN datasets d ON d.dataset_id=i.dataset_id{where} ORDER BY i.{sort_column} {order},i.ingest_run_id {order} LIMIT %s OFFSET %s",
                (*params, limit, offset),
            )
            run_ids = [row[0] for row in cur.fetchall()]
        return tuple(self.get(ingest_run_id) for ingest_run_id in run_ids)

    def count_runs(self, *, keyword: str | None = None, dataset_id: str | None = None, status: str | None = None) -> int:
        where, params = self._filters(keyword=keyword, dataset_id=dataset_id, status=status)
        with self.pool.connection() as connection, connection.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM ingest_runs i JOIN datasets d ON d.dataset_id=i.dataset_id{where}", params)
            return int(cur.fetchone()[0])

    def summarize_runs(self, *, keyword: str | None = None, dataset_id: str | None = None, status: str | None = None) -> IngestSummary:
        where, params = self._filters(keyword=keyword, dataset_id=dataset_id, status=status)
        with self.pool.connection() as connection, connection.cursor() as cur:
            cur.execute(
                f"SELECT count(DISTINCT i.ingest_run_id),count(s.scene_id),count(s.scene_id) FILTER (WHERE s.status='completed'),count(s.scene_id) FILTER (WHERE s.status='failed') FROM ingest_runs i JOIN datasets d ON d.dataset_id=i.dataset_id LEFT JOIN ingest_run_scenes s ON s.ingest_run_id=i.ingest_run_id{where}",
                params,
            )
            row = cur.fetchone()
        return IngestSummary(
            run_count=int(row[0]), scene_count=int(row[1]), completed_scene_count=int(row[2]), failed_scene_count=int(row[3])
        )

    def start_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        already_completed = False
        with self.pool.connection() as connection, connection.cursor() as cur:
            self._lock_run(cur, ingest_run_id)
            cur.execute(
                "UPDATE ingest_run_scenes SET status='running',attempt_count=attempt_count+1,error_message=NULL,updated_at=now() WHERE ingest_run_id=%s AND scene_id=%s AND status IN ('pending','queued')",
                (ingest_run_id, scene_id),
            )
            if cur.rowcount != 1:
                cur.execute("SELECT status FROM ingest_run_scenes WHERE ingest_run_id=%s AND scene_id=%s", (ingest_run_id, scene_id))
                current = cur.fetchone()
                if current is not None and current[0] == "completed":
                    already_completed = True
                else:
                    self._raise_scene_transition(cur, ingest_run_id, scene_id, "start")
            if not already_completed:
                cur.execute(
                    "UPDATE ingest_runs SET status='running',started_at=COALESCE(started_at,now()),completed_at=NULL WHERE ingest_run_id=%s",
                    (ingest_run_id,),
                )
        return self.get(ingest_run_id)

    def complete_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        return self._finish_scene(ingest_run_id, scene_id, "completed", None)

    def fail_scene(self, ingest_run_id: str, scene_id: str, error_message: str) -> IngestRun:
        if not error_message.strip():
            raise ValueError("error_message must not be empty")
        return self._finish_scene(ingest_run_id, scene_id, "failed", error_message)

    def retry_failed(self, ingest_run_id: str, band_unit_ids: tuple[str, ...] | None = None, *, requested_by: str = "system") -> IngestRun:
        with self.pool.connection() as connection, connection.cursor() as cur:
            self._lock_run(cur, ingest_run_id)
            if band_unit_ids is not None and not band_unit_ids:
                raise IngestSceneNotFound("")
            selected_sql = " AND band_unit_ids ?| %s::text[]" if band_unit_ids is not None else ""
            params: tuple[Any, ...] = (ingest_run_id, list(band_unit_ids)) if band_unit_ids is not None else (ingest_run_id,)
            cur.execute(
                f"SELECT scene_id,band_unit_ids,status,error_message,attempt_count,provenance FROM ingest_run_scenes WHERE ingest_run_id=%s{selected_sql} FOR UPDATE",
                params,
            )
            selected = cur.fetchall()
            if band_unit_ids is not None:
                selected_band_units = {band for row in selected for band in _json_array(row[1])}
                if selected_band_units != set(band_unit_ids):
                    raise IngestSceneNotFound(",".join(sorted(set(band_unit_ids) - selected_band_units)))
                if any(row[2] != "failed" for row in selected):
                    raise InvalidIngestTransition("only failed band units can be retried")
            failed = [row for row in selected if row[2] == "failed"]
            if not failed:
                raise InvalidIngestTransition("ingest run has no failed band units to retry")
            now = _now()
            for scene_id, _bands, _status, error_message, attempt_count, raw_provenance in failed:
                provenance = _json_object(raw_provenance)
                history = list(provenance.get("retry_history") or ())
                history.append(
                    {
                        "error_message": error_message,
                        "retried_by": requested_by,
                        "retried_at": now.isoformat(),
                        "attempt_count": int(attempt_count),
                    }
                )
                provenance["retry_history"] = history
                cur.execute(
                    "UPDATE ingest_run_scenes SET status='queued',provenance=%s,error_message=NULL,updated_at=now() WHERE ingest_run_id=%s AND scene_id=%s AND status='failed'",
                    (Jsonb(provenance), ingest_run_id, scene_id),
                )
                if cur.rowcount != 1:
                    raise InvalidIngestTransition(f"failed band unit changed while retrying: {scene_id}")
            cur.execute(
                "UPDATE ingest_runs SET status='queued',error_message=NULL,completed_at=NULL WHERE ingest_run_id=%s", (ingest_run_id,)
            )
        return self.get(ingest_run_id)

    def cancel(self, ingest_run_id: str, reason: str = "") -> IngestRun:
        with self.pool.connection() as connection, connection.cursor() as cur:
            status = self._lock_run(cur, ingest_run_id)
            if status in {"completed", "partial_failure", "failed", "cancelled"}:
                raise InvalidIngestTransition(f"cannot cancel ingest run in status {status}")
            cur.execute(
                "UPDATE ingest_run_scenes SET status='cancelled',updated_at=now() WHERE ingest_run_id=%s AND status IN ('pending','queued','running')",
                (ingest_run_id,),
            )
            cur.execute(
                "SELECT attributes FROM ingest_runs WHERE ingest_run_id=%s",
                (ingest_run_id,),
            )
            attributes = _json_object(cur.fetchone()[0])
            attributes["cancel_reason"] = reason
            cur.execute(
                "UPDATE ingest_runs SET status='cancelled',completed_at=now(),attributes=%s WHERE ingest_run_id=%s",
                (Jsonb(attributes), ingest_run_id),
            )
        return self.get(ingest_run_id)

    def claim_queued_outputs(self, *, limit: int = 10) -> tuple[dict[str, Any], ...]:
        """Claim complete Dataset/output groups, including abandoned leases."""
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        claimed: list[dict[str, Any]] = []
        with self.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT ir.dataset_id,irs.output_version,min(irs.created_at) AS first_created
                FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id
                WHERE (irs.status='queued' AND ir.status IN ('queued','running'))
                   OR (irs.status='running' AND ir.status='running' AND irs.updated_at < now() - interval '5 minutes')
                GROUP BY ir.dataset_id,irs.output_version ORDER BY first_created LIMIT %s
                """,
                (limit,),
            )
            groups = cur.fetchall()
            for group in groups:
                claim_token = uuid4().hex
                cur.execute(
                    """
                    SELECT irs.ingest_run_id,irs.scene_id,ir.dataset_id,irs.output_version,irs.band_unit_ids
                    FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id
                    WHERE ir.dataset_id=%s AND irs.output_version=%s
                      AND ((irs.status='queued' AND ir.status IN ('queued','running'))
                        OR (irs.status='running' AND ir.status='running' AND irs.updated_at < now() - interval '5 minutes'))
                    ORDER BY irs.created_at FOR UPDATE SKIP LOCKED
                    """,
                    (group["dataset_id"], group["output_version"]),
                )
                rows = cur.fetchall()
                items: list[dict[str, str]] = []
                for row in rows:
                    cur.execute(
                        "UPDATE ingest_run_scenes SET status='running',attempt_count=attempt_count+1,error_message=NULL,"
                        "provenance=COALESCE(provenance,'{}'::jsonb) || %s,updated_at=now() "
                        "WHERE ingest_run_id=%s AND scene_id=%s AND status IN ('queued','running')",
                        (
                            Jsonb({"ingest_claim_token": claim_token, "ingest_claimed_at": _now().isoformat()}),
                            row["ingest_run_id"], row["scene_id"],
                        ),
                    )
                    if cur.rowcount != 1:
                        continue
                    cur.execute(
                        "UPDATE ingest_runs SET status='running',started_at=COALESCE(started_at,now()),completed_at=NULL,error_message=NULL "
                        "WHERE ingest_run_id=%s AND status IN ('queued','running')",
                        (row["ingest_run_id"],),
                    )
                    items.append({
                        "ingest_run_id": str(row["ingest_run_id"]), "scene_id": str(row["scene_id"]),
                        "dataset_id": str(row["dataset_id"]), "output_version": str(row["output_version"]),
                        "band_unit_ids": list(_json_array(row["band_unit_ids"])),
                    })
                if items:
                    claimed.append({
                        "dataset_id": str(group["dataset_id"]),
                        "output_version": str(group["output_version"]),
                        "claim_token": claim_token,
                        "items": tuple(items),
                    })
        return tuple(claimed)

    def complete_claimed_output(
        self,
        connection: Any,
        items: tuple[dict[str, str], ...],
        claim_token: str,
    ) -> None:
        with connection.cursor() as cur:
            run_ids: set[str] = set()
            for item in items:
                cur.execute(
                    "SELECT status,provenance->>'ingest_claim_token' FROM ingest_run_scenes "
                    "WHERE ingest_run_id=%s AND scene_id=%s FOR UPDATE",
                    (item["ingest_run_id"], item["scene_id"]),
                )
                row = cur.fetchone()
                if row is None or row[0] != "running" or str(row[1] or "") != claim_token:
                    raise InvalidIngestTransition("ingest claim was cancelled, expired, or replaced")
                run_ids.add(item["ingest_run_id"])
            for item in items:
                cur.execute(
                    "UPDATE ingest_run_scenes SET status='completed',error_message=NULL,updated_at=now() "
                    "WHERE ingest_run_id=%s AND scene_id=%s",
                    (item["ingest_run_id"], item["scene_id"]),
                )
                cur.execute(
                    "UPDATE partition_data_unit_grid_status SET ingest_status='completed',error_message=NULL,updated_at=now() "
                    "WHERE dataset_id=%s AND scene_id=%s AND output_version=%s AND quality_status IN ('pass','warn') "
                    "AND (%s::text[]='{}'::text[] OR band_unit_id=ANY(%s))",
                    (item["dataset_id"], item["scene_id"], item["output_version"], item["band_unit_ids"], item["band_unit_ids"]),
                )
            for ingest_run_id in run_ids:
                self._refresh(cur, ingest_run_id)

    def fail_claimed_output(
        self,
        items: tuple[dict[str, str], ...],
        claim_token: str,
        error_message: str,
    ) -> None:
        with self.pool.connection() as connection, connection.cursor() as cur:
            run_ids: set[str] = set()
            for item in items:
                cur.execute(
                    "UPDATE ingest_run_scenes SET status='failed',error_message=%s,updated_at=now() "
                    "WHERE ingest_run_id=%s AND scene_id=%s AND status='running' "
                    "AND provenance->>'ingest_claim_token'=%s",
                    (error_message, item["ingest_run_id"], item["scene_id"], claim_token),
                )
                if cur.rowcount:
                    run_ids.add(item["ingest_run_id"])
                    cur.execute(
                        "UPDATE partition_data_unit_grid_status SET ingest_status='failed',error_message=%s,updated_at=now() "
                        "WHERE dataset_id=%s AND scene_id=%s AND output_version=%s "
                        "AND (%s::text[]='{}'::text[] OR band_unit_id=ANY(%s))",
                        (error_message, item["dataset_id"], item["scene_id"], item["output_version"], item["band_unit_ids"], item["band_unit_ids"]),
                    )
            for ingest_run_id in run_ids:
                self._refresh(cur, ingest_run_id)

    def _finish_scene(self, ingest_run_id: str, scene_id: str, status: str, error_message: str | None) -> IngestRun:
        already_completed = False
        with self.pool.connection() as connection, connection.cursor() as cur:
            self._lock_run(cur, ingest_run_id)
            cur.execute(
                "UPDATE ingest_run_scenes SET status=%s,error_message=%s,updated_at=now() WHERE ingest_run_id=%s AND scene_id=%s AND status='running'",
                (status, error_message, ingest_run_id, scene_id),
            )
            if cur.rowcount != 1:
                cur.execute("SELECT status FROM ingest_run_scenes WHERE ingest_run_id=%s AND scene_id=%s", (ingest_run_id, scene_id))
                current = cur.fetchone()
                if status == "completed" and current is not None and current[0] == "completed":
                    already_completed = True
                else:
                    self._raise_scene_transition(cur, ingest_run_id, scene_id, "finish")
            if not already_completed:
                cur.execute(
                    "UPDATE partition_data_unit_grid_status SET ingest_status=%s,error_message=%s,updated_at=now() "
                    "WHERE scene_id=%s AND output_version=(SELECT output_version FROM ingest_run_scenes "
                    "WHERE ingest_run_id=%s AND scene_id=%s) "
                    "AND (COALESCE((SELECT jsonb_array_length(band_unit_ids) FROM ingest_run_scenes "
                    "WHERE ingest_run_id=%s AND scene_id=%s),0)=0 OR band_unit_id IN "
                    "(SELECT jsonb_array_elements_text(band_unit_ids) FROM ingest_run_scenes "
                    "WHERE ingest_run_id=%s AND scene_id=%s))",
                    (status, error_message, scene_id, ingest_run_id, scene_id, ingest_run_id, scene_id, ingest_run_id, scene_id),
                )
                self._refresh(cur, ingest_run_id)
        return self.get(ingest_run_id)

    @staticmethod
    def _lock_run(cur: Any, ingest_run_id: str) -> str:
        cur.execute("SELECT status FROM ingest_runs WHERE ingest_run_id=%s FOR UPDATE", (ingest_run_id,))
        row = cur.fetchone()
        if row is None:
            raise IngestRunNotFound(ingest_run_id)
        return str(row[0])

    @staticmethod
    def _refresh(cur: Any, ingest_run_id: str) -> None:
        cur.execute("SELECT status,error_message FROM ingest_run_scenes WHERE ingest_run_id=%s", (ingest_run_id,))
        rows = cur.fetchall()
        status = _aggregate_status([row[0] for row in rows])
        error = next((row[1] for row in rows if row[1]), None) if status in {"failed", "partial_failure"} else None
        cur.execute(
            "UPDATE ingest_runs SET status=%s,error_message=%s,completed_at=CASE WHEN %s IN ('completed','partial_failure','failed','cancelled') THEN now() ELSE NULL END WHERE ingest_run_id=%s",
            (status, error, status, ingest_run_id),
        )

    @staticmethod
    def _raise_scene_transition(cur: Any, ingest_run_id: str, scene_id: str, action: str) -> None:
        cur.execute("SELECT status FROM ingest_run_scenes WHERE ingest_run_id=%s AND scene_id=%s", (ingest_run_id, scene_id))
        row = cur.fetchone()
        if row is None:
            raise IngestSceneNotFound(scene_id)
        raise InvalidIngestTransition(f"cannot {action} scene in status {row[0]}")

    @staticmethod
    def _model(run: dict[str, Any], scenes: list[dict[str, Any]]) -> IngestRun:
        models = []
        for row in scenes:
            value = dict(row)
            provenance = value.pop("provenance", None) or {}
            if isinstance(provenance, str):
                provenance = json.loads(provenance)
            source_batches = provenance.get("source_load_batch_ids")
            if source_batches is None and provenance.get("source_load_batch_id"):
                source_batches = [provenance["source_load_batch_id"]]
            models.append(
                IngestRunScene.model_validate(
                    {
                        **value,
                        "quality_run_id": provenance.get("quality_run_id"),
                        "source_load_batch_ids": tuple(source_batches or ()),
                        "band_unit_ids": tuple(_json_array(value.get("band_unit_ids"))),
                        "retry_history": tuple(provenance.get("retry_history", ())),
                    }
                )
            )
        run_value = dict(run)
        attributes = run_value.pop("attributes", None) or {}
        if isinstance(attributes, str):
            attributes = json.loads(attributes)
        run_value["cancel_reason"] = attributes.get("cancel_reason")
        return IngestRun.model_validate({**run_value, "scenes": tuple(models)})

    @staticmethod
    def _filters(*, keyword: str | None, dataset_id: str | None, status: str | None) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if keyword:
            clauses.append("(i.ingest_run_id ILIKE %s OR i.dataset_id ILIKE %s OR d.dataset_code ILIKE %s)")
            term = f"%{keyword.strip()}%"
            params.extend((term, term, term))
        if dataset_id:
            clauses.append("i.dataset_id=%s")
            params.append(dataset_id)
        if status:
            clauses.append("i.status=%s")
            params.append(status)
        return (f" WHERE {' AND '.join(clauses)}" if clauses else ""), params


def _validate_page(limit: int, offset: int) -> None:
    if not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    if offset < 0:
        raise ValueError("offset must be non-negative")


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return dict(json.loads(value))
    return dict(value)


def _json_array(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = json.loads(value)
    return tuple(str(item) for item in value)


def _validate_sort(sort_by: str, sort_order: str) -> tuple[str, str]:
    if sort_by not in {"created_at", "started_at", "completed_at", "status"}:
        raise ValueError("invalid ingest run sort")
    if sort_order not in {"asc", "desc"}:
        raise ValueError("sort_order must be asc or desc")
    return sort_by, sort_order.upper()
