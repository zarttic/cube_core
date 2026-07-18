from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cube_web.services.ingest_contracts import CreateIngestRun, IngestSceneInput
from cube_web.services.ingest_repository import scene_idempotency_key
from cube_web.services.quality_repository import require_open_gauss_domain_store


@dataclass(frozen=True)
class AutoIngestPolicy:
    allow_warn: bool = False


class ManualIngestRejected(RuntimeError):
    pass


def request_manual_ingest_collection(partition_run_id: str, scene_ids: set[str], *, requested_by: str) -> dict[str, Any]:
    """Create ingest runs for the selected scenes in one partition collection."""
    if not scene_ids:
        raise ManualIngestRejected("at least one partitioned data unit must be selected")
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT prs.scene_id, prs.dataset_id, prs.output_version,
                       q.quality_run_id, q.status AS quality_status
                FROM partition_run_scenes prs
                JOIN partition_runs pr ON pr.partition_run_id=prs.partition_run_id AND pr.status='completed'
                LEFT JOIN LATERAL (
                  SELECT quality_run_id, status FROM partition_quality_runs
                  WHERE dataset_id=prs.dataset_id AND output_version=prs.output_version
                    AND status IN ('pass','warn') AND result_complete=true
                  ORDER BY completed_at DESC NULLS LAST, created_at DESC LIMIT 1
                ) q ON true
                WHERE prs.partition_run_id=%s AND prs.status='completed' AND prs.scene_id=ANY(%s)
                """,
                (partition_run_id, sorted(scene_ids)),
            )
            rows = cur.fetchall()
        if not rows:
            raise ManualIngestRejected("selected data units are not completed partition outputs")
        if any(row["quality_run_id"] is None for row in rows):
            raise ManualIngestRejected("all selected data units must pass quality before ingest")
        grouped: dict[tuple[str, str, str, str], set[str]] = {}
        for row in rows:
            grouped.setdefault((str(row["dataset_id"]), str(row["output_version"]), str(row["quality_run_id"]), str(row["quality_status"])), set()).add(str(row["scene_id"]))
        created = 0
        for (dataset_id, output_version, quality_run_id, quality_status), selected in grouped.items():
            created += create_ingest_runs_after_quality(
                tx, quality_run_id=UUID(quality_run_id), dataset_id=dataset_id,
                output_version=output_version, quality_status=quality_status,
                requested_by=requested_by, manual=True, scene_ids=selected,
            )
        return {"partition_run_id": partition_run_id, "selected_scene_count": len(rows), "created": created}


def request_manual_ingest(dataset_id: str, *, requested_by: str) -> dict[str, Any]:
    """Queue the current quality-approved output only after an explicit action."""
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT d.status,
                       COALESCE(pd.current_output_version, d.current_output_version) AS output_version,
                       q.quality_run_id, q.status AS quality_status
                FROM datasets d
                LEFT JOIN partition_datasets pd ON pd.dataset_id=d.dataset_id
                LEFT JOIN LATERAL (
                  SELECT quality_run_id, status FROM partition_quality_runs
                  WHERE dataset_id=d.dataset_id
                    AND output_version=COALESCE(pd.current_output_version, d.current_output_version)
                    AND status IN ('pass','warn') AND result_complete=true
                  ORDER BY completed_at DESC NULLS LAST, created_at DESC
                  LIMIT 1
                ) q ON true
                WHERE d.dataset_id=%s
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise ManualIngestRejected(f"dataset not found: {dataset_id}")
        if row["status"] != "active" or not row["output_version"] or not row["quality_run_id"]:
            raise ManualIngestRejected("manual ingest requires a current output that passed quality")
        created = create_ingest_runs_after_quality(
            tx,
            quality_run_id=row["quality_run_id"],
            dataset_id=dataset_id,
            output_version=str(row["output_version"]),
            quality_status=str(row["quality_status"]),
            requested_by=requested_by,
            manual=True,
        )
        return {"dataset_id": dataset_id, "output_version": str(row["output_version"]), "created": created}


@dataclass(frozen=True)
class DatasetAutoIngestState:
    dataset_id: str
    status: str
    auto_ingest_allowed: bool
    current_output_version: str | None
    allow_warn_auto_ingest: bool = False


@dataclass(frozen=True)
class PartitionSceneOutput:
    partition_run_id: str
    scene_id: str
    dataset_id: str
    output_version: str | None
    status: str
    source_load_batch_ids: tuple[str, ...] = ()


def plan_ingest_requests(
    *,
    quality_run_id: str,
    quality_status: str,
    dataset: DatasetAutoIngestState,
    partition_scenes: Iterable[PartitionSceneOutput],
    policy: AutoIngestPolicy = AutoIngestPolicy(),
    manual: bool = False,
) -> tuple[CreateIngestRun, ...]:
    accepted_statuses = {"pass", "warn"} if policy.allow_warn or dataset.allow_warn_auto_ingest else {"pass"}
    if quality_status not in accepted_statuses:
        return ()
    if dataset.status != "active" or (not manual and not dataset.auto_ingest_allowed) or dataset.current_output_version is None:
        return ()

    grouped: dict[str, list[IngestSceneInput]] = {}
    for row in partition_scenes:
        if (
            row.dataset_id != dataset.dataset_id
            or row.output_version != dataset.current_output_version
            or row.status != "completed"
        ):
            continue
        grouped.setdefault(row.partition_run_id, []).append(
            IngestSceneInput(
                scene_id=row.scene_id,
                output_version=dataset.current_output_version,
                quality_run_id=quality_run_id,
                source_load_batch_ids=tuple(sorted(set(row.source_load_batch_ids))),
            )
        )

    return tuple(
        CreateIngestRun(
            partition_run_id=partition_run_id,
            dataset_id=dataset.dataset_id,
            scenes=tuple(sorted(scenes, key=lambda scene: scene.scene_id)),
            requested_by="system:quality-gate",
        )
        for partition_run_id, scenes in sorted(grouped.items())
        if scenes
    )


class InMemoryQualityIngestBridge:
    """Small deterministic dispatcher used to verify terminal-gate behavior."""

    def __init__(self) -> None:
        self.requests: dict[str, CreateIngestRun] = {}

    def dispatch(self, requests: Iterable[CreateIngestRun]) -> tuple[CreateIngestRun, ...]:
        for request in requests:
            key = _request_key(request)
            self.requests.setdefault(key, request)
        return tuple(self.requests[key] for key in sorted(self.requests))


class _ConcurrentRequestAlreadyOwned(RuntimeError):
    pass


def create_ingest_runs_after_quality(
    tx: Any,
    *,
    quality_run_id: UUID,
    dataset_id: str,
    output_version: str,
    quality_status: str,
    policy: AutoIngestPolicy = AutoIngestPolicy(),
    requested_by: str = "system:quality-gate",
    manual: bool = False,
    scene_ids: set[str] | None = None,
) -> int:
    """Create managed ingest intents in the quality-completion transaction."""
    if quality_status not in {"pass", "warn"}:
        return 0
    _require_domain_tables(tx)

    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            UPDATE datasets d SET current_output_version=%s,updated_at=now()
            WHERE d.dataset_id=%s
              AND EXISTS (
                SELECT 1 FROM partition_datasets pd
                WHERE pd.dataset_id=d.dataset_id
                  AND pd.current_output_version=%s
              )
              AND EXISTS (
                SELECT 1 FROM partition_run_scenes prs
                JOIN partition_runs pr ON pr.partition_run_id=prs.partition_run_id
                WHERE prs.dataset_id=d.dataset_id AND prs.output_version=%s
                  AND prs.status='completed' AND pr.status='completed'
              )
            """,
            (output_version, dataset_id, output_version, output_version),
        )
        cur.execute(
            "SELECT dataset_id,status,auto_ingest_allowed,current_output_version,attributes "
            "FROM datasets WHERE dataset_id=%s FOR UPDATE",
            (dataset_id,),
        )
        row = cur.fetchone()
        if row is None:
            return 0
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            attributes = json.loads(attributes)
        dataset = DatasetAutoIngestState(
            dataset_id=str(row["dataset_id"]),
            status=str(row["status"]),
            auto_ingest_allowed=bool(row["auto_ingest_allowed"]),
            current_output_version=row["current_output_version"],
            allow_warn_auto_ingest=attributes.get("auto_ingest_allow_warn") is True,
        )
        if dataset.current_output_version != output_version:
            return 0

        cur.execute(
            "SELECT prs.partition_run_id,prs.scene_id,prs.dataset_id,prs.output_version,prs.status "
            "FROM partition_run_scenes prs JOIN partition_runs pr ON pr.partition_run_id=prs.partition_run_id "
            "WHERE prs.dataset_id=%s AND prs.output_version=%s AND prs.status='completed' "
            "AND pr.status='completed' ORDER BY prs.partition_run_id,prs.scene_id",
            (dataset_id, output_version),
        )
        completed_rows = cur.fetchall()
        if scene_ids is not None:
            completed_rows = [row for row in completed_rows if str(row["scene_id"]) in scene_ids]
        if not completed_rows:
            return 0
        scene_ids = sorted({str(item["scene_id"]) for item in completed_rows})
        cur.execute(
            "SELECT scene_id,load_batch_id FROM load_batch_scenes WHERE scene_id=ANY(%s) ORDER BY scene_id,load_batch_id",
            (scene_ids,),
        )
        source_batches: dict[str, list[str]] = {}
        for source in cur.fetchall():
            source_batches.setdefault(str(source["scene_id"]), []).append(str(source["load_batch_id"]))

        candidates = [
            PartitionSceneOutput(
                partition_run_id=str(item["partition_run_id"]),
                scene_id=str(item["scene_id"]),
                dataset_id=str(item["dataset_id"]),
                output_version=item["output_version"],
                status=str(item["status"]),
                source_load_batch_ids=tuple(source_batches.get(str(item["scene_id"]), ())),
            )
            for item in completed_rows
        ]
        requests = plan_ingest_requests(
            quality_run_id=str(quality_run_id),
            quality_status=quality_status,
            dataset=dataset,
            partition_scenes=candidates,
            policy=policy,
            manual=manual,
        )
        if manual:
            requests = tuple(request.model_copy(update={"requested_by": requested_by}) for request in requests)
        created = 0
        for request in requests:
            created += _insert_request(cur, request)
        return created


def _insert_request(cur: Any, request: CreateIngestRun) -> int:
    scene_keys = {
        scene.scene_id: scene_idempotency_key(request.dataset_id, scene.scene_id, scene.output_version)
        for scene in request.scenes
    }
    cur.execute(
        "SELECT idempotency_key FROM ingest_run_scenes WHERE idempotency_key=ANY(%s)",
        (list(scene_keys.values()),),
    )
    existing = {str(row["idempotency_key"]) for row in cur.fetchall()}
    pending = tuple(scene for scene in request.scenes if scene_keys[scene.scene_id] not in existing)
    if not pending:
        return 0

    identity = (
        f"{request.dataset_id}\0{request.partition_run_id}\0{pending[0].output_version}"
        f"\0{pending[0].quality_run_id}"
    )
    ingest_run_id = f"ingest-run-auto-{uuid5(NAMESPACE_URL, identity)}"
    try:
        with cur.connection.transaction():
            inserted_run = _ensure_ingest_run(
                cur,
                ingest_run_id=ingest_run_id,
                request=request,
            )
            scene_owners = {
                _ensure_ingest_scene(
                    cur,
                    ingest_run_id=ingest_run_id,
                    request=request,
                    scene=scene,
                    idempotency_key=scene_keys[scene.scene_id],
                )
                for scene in pending
            }
            if inserted_run and ingest_run_id not in scene_owners:
                raise _ConcurrentRequestAlreadyOwned
    except _ConcurrentRequestAlreadyOwned:
        return 0
    return int(inserted_run)


def _ensure_ingest_run(cur: Any, *, ingest_run_id: str, request: CreateIngestRun) -> bool:
    try:
        with cur.connection.transaction():
            cur.execute(
                "INSERT INTO ingest_runs (ingest_run_id,partition_run_id,dataset_id,status,requested_by) "
                "VALUES (%s,%s,%s,'queued',%s)",
                (ingest_run_id, request.partition_run_id, request.dataset_id, request.requested_by),
            )
        return True
    except UniqueViolation:
        cur.execute(
            "SELECT partition_run_id,dataset_id,status FROM ingest_runs WHERE ingest_run_id=%s",
            (ingest_run_id,),
        )
        owner = cur.fetchone()
        if owner is None or (
            str(owner["partition_run_id"]),
            str(owner["dataset_id"]),
        ) != (request.partition_run_id, request.dataset_id):
            raise RuntimeError(f"ingest run identity conflict: {ingest_run_id}")
        if str(owner["status"]) not in {"pending", "queued"}:
            raise RuntimeError(f"cannot append scenes to ingest run in status {owner['status']}: {ingest_run_id}")
        return False


def _ensure_ingest_scene(
    cur: Any,
    *,
    ingest_run_id: str,
    request: CreateIngestRun,
    scene: IngestSceneInput,
    idempotency_key: str,
) -> str:
    try:
        with cur.connection.transaction():
            cur.execute(
                "INSERT INTO ingest_run_scenes "
                "(ingest_run_id,scene_id,partition_run_id,output_version,status,idempotency_key,provenance) "
                "VALUES (%s,%s,%s,%s,'queued',%s,%s)",
                (
                    ingest_run_id,
                    scene.scene_id,
                    request.partition_run_id,
                    scene.output_version,
                    idempotency_key,
                    Jsonb(
                        {
                            "quality_run_id": scene.quality_run_id,
                            "source_load_batch_ids": list(scene.source_load_batch_ids),
                        }
                    ),
                ),
            )
        return ingest_run_id
    except UniqueViolation:
        cur.execute(
            "SELECT s.ingest_run_id,s.scene_id,s.output_version,i.dataset_id FROM ingest_run_scenes s "
            "JOIN ingest_runs i ON i.ingest_run_id=s.ingest_run_id WHERE s.idempotency_key=%s",
            (idempotency_key,),
        )
        owner = cur.fetchone()
        if owner is None or (
            str(owner["scene_id"]),
            str(owner["output_version"]),
            str(owner["dataset_id"]),
        ) != (scene.scene_id, scene.output_version, request.dataset_id):
            raise RuntimeError(f"ingest scene identity conflict: {scene.scene_id}/{scene.output_version}")
        return str(owner["ingest_run_id"])


def _require_domain_tables(tx: Any) -> None:
    required = {
        "datasets",
        "load_batch_scenes",
        "partition_runs",
        "partition_run_scenes",
        "ingest_runs",
        "ingest_run_scenes",
    }
    with tx.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY(%s)",
            (sorted(required),),
        )
        existing = {str(row[0]) for row in cur.fetchall()}
    missing = required - existing
    if missing:
        raise RuntimeError(f"managed ingest schema is incomplete: {', '.join(sorted(missing))}")


def _request_key(request: CreateIngestRun) -> str:
    scene_identity = ",".join(
        f"{scene.scene_id}:{scene.output_version}" for scene in sorted(request.scenes, key=lambda item: item.scene_id)
    )
    return f"{request.dataset_id}:{request.partition_run_id}:{scene_identity}"
