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


@dataclass(frozen=True)
class AutoIngestPolicy:
    allow_warn: bool = False


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
) -> tuple[CreateIngestRun, ...]:
    accepted_statuses = {"pass", "warn"} if policy.allow_warn or dataset.allow_warn_auto_ingest else {"pass"}
    if quality_status not in accepted_statuses:
        return ()
    if dataset.status != "active" or not dataset.auto_ingest_allowed or dataset.current_output_version is None:
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
        )
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
