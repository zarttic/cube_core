"""Batched, resumable dataset deletion.

The dataset delete chain used to run roughly twenty ``DELETE`` statements plus a
synchronous object-store cleanup inside *one* database transaction and *one*
HTTP request.  Measured on 2026-09-19 (global LAI dataset with two output
versions): 790 s, during which every other session waited -- new connections
needed 8-40 s to authenticate and ordinary queries went from 27 ms to 170 s --
because that single transaction held its locks and saturated the I/O path the
whole time.

This module splits the same work into two halves:

``build_deletion_plan``
    Runs in one short transaction while the rows still exist.  It checks the
    guards (running work, unwithdrawn publication) and records every identifier
    the deletion owns -- scene ids, load-batch ids, partition-run ids, output
    versions, ingest job ids -- so a retry after a restart never has to guess
    what an interrupted run already removed.

``execute_deletion_plan``
    Runs the recorded steps as small, individually committed batches and reports
    per-step row counts and seconds.  Every step is a predicate over stable ids,
    so replaying a plan is idempotent: rows that are already gone match nothing.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse

from cube_split.ingest.dataset_cleanup import (
    delete_table_batch,
    existing_managed_tables,
    make_ingest_job_id,
    managed_object_uris,
    managed_table_cleanups,
)

logger = logging.getLogger(__name__)

#: Bump on ANY change to build_steps -- the step list, its order or a predicate. -- the step list, its order or a predicate.
#: A stored plan is re-materialised from its identifiers when this value moves,
#: so already-queued deletions pick up the fix instead of replaying stale SQL
#: forever.  Missed bumps shipped two production bugs on 2026-09-20: a stale FK
#: order and a stale ``load_batch_sources`` predicate.
#: ``test_plan_version_tracks_the_step_definitions`` pins a fingerprint of the
#: step list, so changing steps without bumping this fails the suite.
PLAN_VERSION = 4

DEFAULT_BATCH_SIZE = 20_000
#: Safety valve so a pathological plan cannot loop forever.
MAX_BATCHES_PER_STEP = 200_000

#: Only objects below the dataset's own partition namespace are ever removed.
PARTITION_OBJECT_PREFIX = "partition/{dataset_id}/"

GROUP_INGEST = "ingest"
GROUP_PARTITION = "partition"
GROUP_LOAD_BATCH = "load_batch"
GROUP_SCHEDULER_BATCH = "scheduler_batch"
GROUP_SCENE = "scene"
GROUP_DATASET = "dataset"


class DatasetDeletionRejected(RuntimeError):
    """The deletion may not start (or resume) right now."""

    _CODES = {"not_found", "conflict", "publication"}

    def __init__(self, code: str, message: str) -> None:
        if code not in self._CODES:
            raise ValueError(f"unknown rejection code: {code}")
        super().__init__(message)
        self.code = code


class DatasetDeletionInterrupted(RuntimeError):
    """Execution stopped early (shutdown or lease loss); the plan stays resumable."""


@dataclass(frozen=True)
class DeletionStep:
    """One table-scoped delete: ``DELETE FROM table WHERE predicate``.

    ``predicate``/``params`` come from module constants and from ids resolved by
    :func:`build_deletion_plan`, never from request input, so the statement is
    safe to interpolate.
    """

    name: str
    table: str
    predicate: str = ""
    params: tuple[Any, ...] = ()
    group: str = GROUP_PARTITION
    batch_size: int = DEFAULT_BATCH_SIZE
    #: When set, this statement runs verbatim (once) instead of the batched
    #: ctid DELETE.  Used for pruning a scheduler batch's stored dataset list.
    statement: str = ""

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["params"] = [
            list(value) if isinstance(value, (list, tuple)) else value for value in self.params
        ]
        return payload

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "DeletionStep":
        return cls(
            name=str(payload["name"]),
            table=str(payload["table"]),
            predicate=str(payload["predicate"]),
            params=tuple(
                list(value) if isinstance(value, list) else value
                for value in payload.get("params", ())
            ),
            group=str(payload.get("group", GROUP_PARTITION)),
            batch_size=int(payload.get("batch_size", DEFAULT_BATCH_SIZE)),
            statement=str(payload.get("statement") or ""),
        )


@dataclass
class StepOutcome:
    name: str
    table: str
    group: str
    deleted: int = 0
    batches: int = 0
    seconds: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "table": self.table,
            "group": self.group,
            "deleted": self.deleted,
            "batches": self.batches,
            "seconds": round(self.seconds, 3),
        }


@dataclass
class DeletionPlan:
    dataset_id: str
    dataset_title: str
    requested_by: str
    identifiers: dict[str, list[str]] = field(default_factory=dict)
    object_prefix: str = ""
    steps: tuple[DeletionStep, ...] = ()
    plan_version: int = PLAN_VERSION

    def to_json(self) -> dict[str, Any]:
        return {
            "plan_version": self.plan_version,
            "dataset_id": self.dataset_id,
            "dataset_title": self.dataset_title,
            "requested_by": self.requested_by,
            "identifiers": {key: list(value) for key, value in self.identifiers.items()},
            "object_prefix": self.object_prefix,
            "steps": [step.to_json() for step in self.steps],
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "DeletionPlan":
        return cls(
            dataset_id=str(payload["dataset_id"]),
            dataset_title=str(payload.get("dataset_title") or ""),
            requested_by=str(payload.get("requested_by") or ""),
            identifiers={
                str(key): [str(item) for item in (value or [])]
                for key, value in (payload.get("identifiers") or {}).items()
            },
            object_prefix=str(payload.get("object_prefix") or ""),
            steps=tuple(DeletionStep.from_json(step) for step in payload.get("steps") or ()),
            plan_version=int(payload.get("plan_version") or 1),
        )

    def ids(self, key: str) -> list[str]:
        return list(self.identifiers.get(key) or ())


def partition_object_prefix(dataset_id: str) -> str:
    if not dataset_id or "/" in dataset_id or "\\" in dataset_id or dataset_id in {".", ".."}:
        raise ValueError("dataset_id must be a non-empty path segment")
    return PARTITION_OBJECT_PREFIX.format(dataset_id=dataset_id)


def build_deletion_plan(
    connection: Any,
    *,
    dataset_id: str,
    actor: str,
    collect_object_uris: bool = False,
) -> tuple[DeletionPlan, list[str]]:
    """Resolve and validate everything one dataset deletion owns.

    Returns the plan plus -- only when ``collect_object_uris`` is set -- the exact
    generated objects to remove, which the synchronous caller hands to the
    object-store hook.  The background job removes objects by prefix instead, so
    it never has to materialise one URI per row.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT dataset_id, dataset_title FROM datasets WHERE dataset_id=%s FOR UPDATE",
            (dataset_id,),
        )
        dataset = cursor.fetchone()
        if dataset is None:
            raise DatasetDeletionRejected("not_found", dataset_id)
        dataset_title = str(_row_value(dataset, "dataset_title") or "")

        cursor.execute("SELECT scene_id FROM scenes WHERE dataset_id=%s FOR UPDATE", (dataset_id,))
        scene_ids = _ids(cursor.fetchall(), "scene_id")
        scene_id_array = scene_ids or ["__no_scene__"]

        cursor.execute(
            """SELECT draft_id, submitted_partition_run_id, source_load_batch_ids
                 FROM partition_drafts
                WHERE selection->'datasets' @> %s::jsonb
                FOR UPDATE""",
            (json.dumps([{"dataset_id": dataset_id}]),),
        )
        draft_rows = cursor.fetchall()
        draft_ids = _ids(draft_rows, "draft_id")
        draft_run_ids = _ids(draft_rows, "submitted_partition_run_id")
        draft_load_batch_ids = [
            str(value)
            for row in draft_rows
            for value in (_row_value(row, "source_load_batch_ids") or ())
        ]

        cursor.execute(
            """SELECT DISTINCT load_batch_id FROM load_batch_scenes
               WHERE scene_id=ANY(%s::text[])
               UNION
               SELECT DISTINCT load_batch_id FROM load_batch_sources WHERE source_dataset_id=%s
               UNION
               SELECT DISTINCT source_load_batch_id AS load_batch_id FROM load_batch_sources
               WHERE source_dataset_id=%s""",
            (scene_id_array, dataset_id, dataset_id),
        )
        load_batch_ids = _dedupe([*_ids(cursor.fetchall(), "load_batch_id"), *draft_load_batch_ids])

        # A dataset can appear in several scheduler batches over time: the batch
        # that produced the current output version (partition_datasets.batch_id),
        # every batch whose stored payload still lists it, and the batches behind
        # its output versions (task_id -> partition_job_attempts.batch_id).
        # Resolving only the first one left orphan batches behind (2026-09-20).
        # NOTE: OpenGauss 7.0 has neither jsonb_agg nor jsonb_build_object, so the
        # payload lookup walks jsonb_array_elements instead of building a
        # containment literal (the real-schema test pins this).
        cursor.execute(
            """SELECT DISTINCT batch_id FROM (
                 SELECT batch_id FROM partition_datasets WHERE dataset_id=%s
                 UNION
                 SELECT batch.batch_id FROM partition_batches batch
                  WHERE EXISTS (
                        SELECT 1 FROM jsonb_array_elements(
                                      COALESCE(batch.normalized_payload->'datasets', '[]'::jsonb)) AS entry
                         WHERE entry->>'dataset_id' = %s)
                 UNION
                 SELECT attempt.batch_id
                   FROM partition_output_versions version
                   JOIN partition_job_attempts attempt ON attempt.task_id = version.task_id
                  WHERE version.dataset_id = %s
               ) candidates""",
            (dataset_id, dataset_id, dataset_id),
        )
        scheduler_batch_ids = _ids(cursor.fetchall(), "batch_id")

        cursor.execute(
            "SELECT partition_run_id FROM partition_run_scenes WHERE dataset_id=%s FOR UPDATE",
            (dataset_id,),
        )
        partition_run_ids = _dedupe(
            [*_ids(cursor.fetchall(), "partition_run_id"), *draft_run_ids]
        )
        cursor.execute(
            "SELECT partition_run_id FROM ingest_runs WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
        )
        partition_run_ids = _dedupe([*partition_run_ids, *_ids(cursor.fetchall(), "partition_run_id")])

        if partition_run_ids:
            cursor.execute(
                "SELECT source_load_batch_ids FROM partition_runs WHERE partition_run_id=ANY(%s::text[])",
                (partition_run_ids,),
            )
            run_load_batch_ids = [
                str(value)
                for row in cursor.fetchall()
                for value in (_row_value(row, "source_load_batch_ids") or ())
            ]
            load_batch_ids = _dedupe([*load_batch_ids, *run_load_batch_ids])

        # The rs_* ingest tables carry no foreign key to the dataset, so ownership
        # has to be resolved from identifiers that are about to be deleted: the
        # dataset's own output versions (= rs_cube_cell_fact.cube_version) and the
        # deterministic ingest job ids built from (dataset_id, output_version, band_unit_id).
        cursor.execute(
            """SELECT DISTINCT scene.output_version::text AS output_version,
                      unit.band_unit_id::text AS band_unit_id
                 FROM ingest_run_scenes scene
                 JOIN ingest_runs run ON run.ingest_run_id=scene.ingest_run_id
                 CROSS JOIN LATERAL jsonb_array_elements_text(scene.band_unit_ids) AS unit(band_unit_id)
                WHERE run.dataset_id=%s""",
            (dataset_id,),
        )
        ingest_units = cursor.fetchall()
        cursor.execute(
            "SELECT DISTINCT output_version::text AS output_version FROM partition_output_versions WHERE dataset_id=%s",
            (dataset_id,),
        )
        output_versions = sorted({
            str(_row_value(row, "output_version"))
            for row in [*ingest_units, *cursor.fetchall()]
            if _row_value(row, "output_version")
        })
        ingest_job_ids = sorted({
            make_ingest_job_id(
                dataset_id,
                str(_row_value(row, "output_version")),
                str(_row_value(row, "band_unit_id")),
            )
            for row in ingest_units
            if _row_value(row, "output_version") and _row_value(row, "band_unit_id")
        })

        identifiers = {
            "scene_ids": scene_ids,
            "draft_ids": draft_ids,
            "load_batch_ids": load_batch_ids,
            "scheduler_batch_ids": scheduler_batch_ids,
            "partition_run_ids": partition_run_ids,
            "output_versions": output_versions,
            "ingest_job_ids": ingest_job_ids,
        }
        _assert_deletion_allowed(
            cursor,
            dataset_id=dataset_id,
            scene_ids=scene_ids,
            load_batch_ids=load_batch_ids,
            scheduler_batch_ids=scheduler_batch_ids,
            partition_run_ids=partition_run_ids,
        )

    plan = DeletionPlan(
        dataset_id=dataset_id,
        dataset_title=dataset_title,
        requested_by=actor,
        identifiers=identifiers,
        object_prefix=partition_object_prefix(dataset_id),
        steps=build_steps(connection, dataset_id=dataset_id, identifiers=identifiers),
    )
    object_uris = _collect_object_uris(connection, plan) if collect_object_uris else []
    logger.info(
        "dataset_deletion.plan_ready dataset_id=%s scenes=%s runs=%s versions=%s steps=%s",
        dataset_id, len(scene_ids), len(partition_run_ids), len(output_versions), len(plan.steps),
    )
    return plan, object_uris


def refresh_deletion_steps(connection: Any, plan: DeletionPlan) -> DeletionPlan:
    """Re-materialise a stored plan's steps with the current engine.

    Identifiers resolved at request time stay authoritative (rows they refer to
    may already be gone), but the step list comes from the running code, so a
    queued job cannot replay an outdated delete order.
    """
    return DeletionPlan(
        dataset_id=plan.dataset_id,
        dataset_title=plan.dataset_title,
        requested_by=plan.requested_by,
        identifiers=plan.identifiers,
        object_prefix=plan.object_prefix,
        steps=build_steps(connection, dataset_id=plan.dataset_id, identifiers=plan.identifiers),
        plan_version=PLAN_VERSION,
    )


def build_steps(
    connection: Any,
    *,
    dataset_id: str,
    identifiers: Mapping[str, Sequence[str]],
) -> tuple[DeletionStep, ...]:
    """Return the ordered delete steps for a resolved dataset.

    The order is foreign-key safe, and ``partition_indexes`` precedes
    ``partition_tiles`` on purpose: deleting tiles first would fire one
    referential check per tile row.
    """
    scene_ids = list(identifiers.get("scene_ids") or ())
    draft_ids = list(identifiers.get("draft_ids") or ())
    load_batch_ids = list(identifiers.get("load_batch_ids") or ())
    scheduler_batch_ids = list(identifiers.get("scheduler_batch_ids") or ())
    partition_run_ids = list(identifiers.get("partition_run_ids") or ())
    ingest_job_ids = list(identifiers.get("ingest_job_ids") or ())
    output_versions = list(identifiers.get("output_versions") or ())

    steps: list[DeletionStep] = []

    known_rs_tables = existing_managed_tables(connection)
    for cleanup in managed_table_cleanups(
        dataset_id, run_ids=ingest_job_ids, output_versions=output_versions
    ):
        if cleanup.table not in known_rs_tables:
            continue
        steps.append(
            DeletionStep(
                name=f"ingest.{cleanup.table}",
                table=cleanup.table,
                predicate=cleanup.predicate,
                params=tuple(cleanup.params),
                group=GROUP_INGEST,
            )
        )

    # Quality/publication rows must be removed before their output and dataset
    # foreign-key parents.  partition_quality_runs itself is NOT deleted here:
    # partition_datasets.current_quality_run_id references it through a DEFERRABLE
    # constraint, and a batched deletion commits between steps (see below).
    for table, name in (
        ("partition_quality_errors", "partition.quality_errors"),
        ("partition_quality_results", "partition.quality_results"),
        ("partition_quality_warn_approvals", "partition.quality_warn_approvals"),
        ("partition_publication_targets", "partition.publication_targets"),
        ("partition_publications", "partition.publications"),
        ("partition_domain_outbox", "partition.domain_outbox"),
    ):
        steps.append(_dataset_step(table, name=name, dataset_id=dataset_id))

    # Ingest records reference both scenes and partition runs, so the scene rows
    # go before the run rows they point at.
    steps.append(
        DeletionStep(
            name="partition.ingest_run_scenes",
            table="ingest_run_scenes",
            predicate="ingest_run_id IN (SELECT ingest_run_id FROM ingest_runs WHERE dataset_id=%s)",
            params=(dataset_id,),
            group=GROUP_PARTITION,
        )
    )
    for table, name in (
        ("ingest_runs", "partition.ingest_runs"),
        ("partition_data_unit_grid_status", "partition.grid_status"),
        ("partition_run_scenes", "partition.run_scenes"),
    ):
        steps.append(_dataset_step(table, name=name, dataset_id=dataset_id))

    if draft_ids:
        steps.append(
            DeletionStep(
                name="partition.drafts_selected",
                table="partition_drafts",
                predicate="draft_id = ANY(%s::text[])",
                params=(draft_ids,),
            )
        )
    if partition_run_ids:
        steps.append(
            DeletionStep(
                name="partition.drafts_submitted",
                table="partition_drafts",
                predicate=(
                    "submitted_partition_run_id = ANY(%s::text[]) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_run_scenes scene "
                    " WHERE scene.partition_run_id = partition_drafts.submitted_partition_run_id)"
                ),
                params=(partition_run_ids,),
            )
        )
        steps.append(
            DeletionStep(
                name="partition.runs",
                table="partition_runs",
                predicate=(
                    "partition_run_id = ANY(%s::text[]) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_run_scenes scene "
                    " WHERE scene.partition_run_id = partition_runs.partition_run_id) AND NOT EXISTS "
                    "(SELECT 1 FROM ingest_runs ingest "
                    " WHERE ingest.partition_run_id = partition_runs.partition_run_id) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_data_unit_grid_status grid "
                    " WHERE grid.partition_run_id = partition_runs.partition_run_id) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_drafts other_draft "
                    " WHERE other_draft.submitted_partition_run_id = partition_runs.partition_run_id)"
                ),
                params=(partition_run_ids,),
            )
        )

    # Order below follows the foreign-key graph, because every batch commits:
    # a referencing row must be gone before the row it references (the legacy
    # single-transaction version could lean on the two DEFERRABLE constraints on
    # partition_datasets, which a batched deletion cannot).  Concretely:
    #   * partition_indexes / partition_tiles / partition_publication_targets
    #     reference partition_dataset_bands (NO ACTION), so they go first;
    #   * deleting partition_datasets cascades to partition_dataset_assets and
    #     partition_dataset_bands, and it is the row that references
    #     partition_quality_runs and partition_output_versions (DEFERRED), so it
    #     must precede both of them.
    for table, name in (
        ("partition_indexes", "partition.indexes"),
        ("partition_tiles", "partition.tiles"),
        ("partition_datasets", "partition.datasets"),
        ("partition_quality_runs", "partition.quality_runs"),
        ("partition_grid_cells", "partition.grid_cells"),
        ("partition_output_chunks", "partition.output_chunks"),
        ("partition_logical_staging_rows", "partition.staging_rows"),
        ("partition_output_versions", "partition.output_versions"),
    ):
        steps.append(_dataset_step(table, name=name, dataset_id=dataset_id))

    # partition_assets carries the partition material state per scene and has no
    # dataset column, so it is scoped by the dataset's scenes.
    if scene_ids:
        steps.append(
            DeletionStep(
                name="partition.assets",
                table="partition_assets",
                predicate="scene_id = ANY(%s::text[])",
                params=(scene_ids,),
                group=GROUP_PARTITION,
            )
        )

    if scene_ids:
        steps.append(
            DeletionStep(
                name="load.load_batch_scenes",
                table="load_batch_scenes",
                predicate="scene_id = ANY(%s::text[])",
                params=(scene_ids,),
                group=GROUP_LOAD_BATCH,
            )
        )
    # load_batch_sources keys the dataset as source_dataset_id (there is no
    # dataset_id column): caught by the pre-flight validation below.
    steps.append(
        DeletionStep(
            name="load.load_batch_sources",
            table="load_batch_sources",
            predicate="source_dataset_id=%s",
            params=(dataset_id,),
            group=GROUP_LOAD_BATCH,
        )
    )
    if load_batch_ids:
        steps.append(
            DeletionStep(
                name="load.load_batches",
                table="load_batches",
                predicate=(
                    "load_batch_id = ANY(%s::text[]) AND NOT EXISTS "
                    "(SELECT 1 FROM load_batch_scenes scene "
                    " WHERE scene.load_batch_id = load_batches.load_batch_id) AND NOT EXISTS "
                    "(SELECT 1 FROM load_batch_sources source "
                    " WHERE source.load_batch_id = load_batches.load_batch_id "
                    "    OR source.source_load_batch_id = load_batches.load_batch_id) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_runs run "
                    " WHERE run.source_load_batch_ids ? load_batches.load_batch_id) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_run_scenes run_scene "
                    " WHERE run_scene.source_load_batch_id = load_batches.load_batch_id) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_drafts draft "
                    " WHERE draft.source_load_batch_ids ? load_batches.load_batch_id)"
                ),
                params=(load_batch_ids,),
                group=GROUP_LOAD_BATCH,
            )
        )

    if scene_ids:
        steps.append(
            DeletionStep(
                name="scene.audit",
                table="scene_dataset_audit",
                predicate="scene_id = ANY(%s::text[]) OR dataset_id=%s OR previous_dataset_id=%s",
                params=(scene_ids, dataset_id, dataset_id),
                group=GROUP_SCENE,
            )
        )
        for table, name in (
            ("scene_bands", "scene.bands"),
            ("scene_assets", "scene.assets"),
            ("scenes", "scene.scenes"),
        ):
            steps.append(
                DeletionStep(
                    name=name,
                    table=table,
                    predicate="scene_id = ANY(%s::text[])",
                    params=(scene_ids,),
                    group=GROUP_SCENE,
                )
            )
    else:
        steps.append(
            DeletionStep(
                name="scene.audit",
                table="scene_dataset_audit",
                predicate="dataset_id=%s OR previous_dataset_id=%s",
                params=(dataset_id, dataset_id),
                group=GROUP_SCENE,
            )
        )

    steps.append(_dataset_step("datasets", name="dataset.datasets", dataset_id=dataset_id, group=GROUP_DATASET))

    if scheduler_batch_ids:
        # A scheduler batch can carry several datasets.  Deleting one of them must
        # remove it from the batch's stored contract; a batch left with no dataset
        # at all is then removed, so it stops showing up in the partition list.
        steps.append(
            DeletionStep(
                name="scheduler.partition_batch_payload",
                table="partition_batches",
                group=GROUP_SCHEDULER_BATCH,
                statement=(
                    "UPDATE partition_batches "
                    "   SET normalized_payload = jsonb_set("
                    "         jsonb_set(normalized_payload, '{datasets}', "
                    "           COALESCE((SELECT json_agg(entry)::jsonb "
                    "                       FROM jsonb_array_elements(normalized_payload->'datasets') AS entry "
                    "                      WHERE entry->>'dataset_id' <> %s), '[]'::jsonb), true), "
                    "         '{dataset_partitions}', "
                    "           COALESCE((SELECT json_agg(entry)::jsonb "
                    "                       FROM jsonb_array_elements(normalized_payload->'dataset_partitions') AS entry "
                    "                      WHERE entry->>'dataset_id' <> %s), '[]'::jsonb), true), "
                    "       updated_at = now() "
                    " WHERE batch_id = ANY(%s::text[]) "
                    "   AND EXISTS (SELECT 1 FROM jsonb_array_elements("
                    "                 COALESCE(normalized_payload->'datasets', '[]'::jsonb)) AS entry "
                    "                WHERE entry->>'dataset_id' = %s)"
                ),
                params=(dataset_id, dataset_id, scheduler_batch_ids, dataset_id),
            )
        )
        steps.append(
            DeletionStep(
                name="scheduler.partition_batches",
                table="partition_batches",
                predicate=(
                    "batch_id = ANY(%s::text[]) AND NOT EXISTS "
                    "(SELECT 1 FROM partition_datasets dataset "
                    " WHERE dataset.batch_id = partition_batches.batch_id) "
                    # Only a batch whose contract no longer holds any dataset goes
                    # away: one shared with another dataset stays.
                    "AND COALESCE(jsonb_array_length(normalized_payload->'datasets'), 0) = 0"
                ),
                params=(scheduler_batch_ids,),
                group=GROUP_SCHEDULER_BATCH,
            )
        )

    return tuple(steps)


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #


def _rollback(connection: Any) -> None:
    """Clear an aborted transaction so the caller can still record the failure."""
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:  # pragma: no cover - a broken connection is reported upstream
            pass


def validate_deletion_plan(connection: Any, plan: DeletionPlan) -> list[tuple[str, str]]:
    """EXPLAIN every step before anything is deleted.

    The plan is data stored in a job row, so it can name a table or column that
    a later schema does not have (2026-09-20: ``load_batch_sources.dataset_id``,
    which errored after thousands of rows were already removed).  EXPLAIN checks
    the statements against the live catalog without touching a single row.
    Returns ``[(step_name, error), ...]``; empty means the plan is executable.
    """
    failures: list[tuple[str, str]] = []
    for step in plan.steps:
        if step.statement:
            statement = f"EXPLAIN {step.statement}"
        else:
            statement = (
                f"EXPLAIN DELETE FROM {step.table} WHERE ctid IN "
                f"(SELECT ctid FROM {step.table} WHERE {step.predicate} LIMIT {int(step.batch_size)})"
            )
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, tuple(step.params))
                cursor.fetchall()
        except Exception as exc:  # the server rejected the statement: plan is stale/broken
            failures.append((step.name, " ".join(str(exc).split())[:200]))
            # A rejected statement aborts the transaction; without this the next
            # EXPLAIN would report "current transaction is aborted" instead of
            # its own error.
            _rollback(connection)
    return failures


def execute_deletion_plan(
    connection: Any,
    plan: DeletionPlan | Mapping[str, Any],
    *,
    batch_size: int | None = None,
    on_step: Callable[[StepOutcome, int], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
    validate: bool = True,
) -> dict[str, Any]:
    """Run every step in committed batches, returning counts and timings.

    ``validate`` first EXPLAINs the whole plan, so a broken step fails before any
    row is removed instead of halfway through the deletion.
    """
    resolved = plan if isinstance(plan, DeletionPlan) else DeletionPlan.from_json(plan)
    if validate:
        failures = validate_deletion_plan(connection, resolved)
        if failures:
            details = "; ".join(f"{name}: {error}" for name, error in failures)
            raise RuntimeError(f"deletion plan failed pre-flight validation ({len(failures)}): {details}")
    started = time.monotonic()
    outcomes: list[StepOutcome] = []
    deleted_rows: dict[str, int] = {}
    deleted_by_table: dict[str, int] = {}
    total = 0

    for step in resolved.steps:
        if should_stop is not None and should_stop():
            raise DatasetDeletionInterrupted("deletion stopped before step %s" % step.name)
        try:
            outcome = _run_step(
                connection,
                step,
                batch_size=batch_size or step.batch_size,
                should_stop=should_stop,
            )
        except DatasetDeletionInterrupted:
            _rollback(connection)
            raise
        except Exception:
            # Leave the connection usable: the worker still has to write the
            # failure back to the job row.
            _rollback(connection)
            raise
        outcomes.append(outcome)
        deleted_rows[step.name] = outcome.deleted
        deleted_by_table[step.table] = deleted_by_table.get(step.table, 0) + outcome.deleted
        total += outcome.deleted
        if on_step is not None:
            on_step(outcome, total)

    return {
        "deleted_rows": deleted_rows,
        "deleted_rows_by_table": deleted_by_table,
        "deleted_total": total,
        "steps": [outcome.to_json() for outcome in outcomes],
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "batch_size": int(batch_size or DEFAULT_BATCH_SIZE),
    }


def _run_step(
    connection: Any,
    step: DeletionStep,
    *,
    batch_size: int,
    should_stop: Callable[[], bool] | None = None,
) -> StepOutcome:
    outcome = StepOutcome(name=step.name, table=step.table, group=step.group)
    started = time.monotonic()
    if step.statement:
        # A maintenance statement (one row set, no batching): still committed and
        # timed like every other step so the job row tells the whole story.
        with connection.cursor() as cursor:
            cursor.execute(step.statement, tuple(step.params))
            outcome.deleted = max(int(cursor.rowcount), 0)
        outcome.batches = 1
        _commit(connection)
        outcome.seconds = time.monotonic() - started
        return outcome
    while True:
        outcome.batches += 1
        # Each batch is its own transaction: the plan is replayable and every step
        # is a predicate over stable ids, so a crash between batches only costs a
        # re-run, while a committed batch can never starve the database again.
        deleted = delete_table_batch(
            connection,
            table=step.table,
            predicate=step.predicate,
            params=step.params,
            batch_size=batch_size,
        )
        _commit(connection)
        outcome.deleted += deleted
        if deleted < batch_size:
            break
        if outcome.batches >= MAX_BATCHES_PER_STEP:
            raise RuntimeError(f"step {step.name} exceeded {MAX_BATCHES_PER_STEP} batches")
        if should_stop is not None and should_stop():
            raise DatasetDeletionInterrupted(f"deletion stopped during step {step.name}")
    outcome.seconds = time.monotonic() - started
    return outcome


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if callable(commit):
        commit()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _dataset_step(table: str, *, name: str, dataset_id: str, group: str = GROUP_PARTITION) -> DeletionStep:
    return DeletionStep(name=name, table=table, predicate="dataset_id=%s", params=(dataset_id,), group=group)


def _assert_deletion_allowed(
    cursor: Any,
    *,
    dataset_id: str,
    scene_ids: Sequence[str],
    load_batch_ids: Sequence[str],
    scheduler_batch_ids: Sequence[str],
    partition_run_ids: Sequence[str],
) -> None:
    load_batch_id_array = list(load_batch_ids) or ["__no_load_batch__"]
    scheduler_batch_id_array = list(scheduler_batch_ids) or ["__no_scheduler_batch__"]
    partition_run_id_array = list(partition_run_ids) or ["__no_partition_run__"]

    cursor.execute(
        """SELECT EXISTS (
             SELECT 1 FROM partition_job_attempts
              WHERE batch_id=ANY(%s::text[])
                AND status IN ('queued','running','retrying','cancel_requested')
             UNION ALL
             SELECT 1 FROM partition_datasets
              WHERE dataset_id=%s
                AND (partition_status IN ('queued','running') OR quality_status='running')
             UNION ALL
             SELECT 1 FROM partition_quality_runs
              WHERE dataset_id=%s AND status IN ('pending','running')
             UNION ALL
             SELECT 1 FROM partition_runs
              WHERE partition_run_id=ANY(%s::text[])
                AND status IN ('pending','queued','running')
             UNION ALL
             SELECT 1 FROM ingest_runs
              WHERE dataset_id=%s AND status IN ('pending','queued','running')
             UNION ALL
             SELECT 1 FROM load_batches
              WHERE load_batch_id=ANY(%s::text[])
                AND status IN ('pending','running')
           ) AS active""",
        (
            scheduler_batch_id_array,
            dataset_id,
            dataset_id,
            partition_run_id_array,
            dataset_id,
            load_batch_id_array,
        ),
    )
    if _row_value(cursor.fetchone(), "active"):
        raise DatasetDeletionRejected(
            "conflict", "数据集存在运行中的剖分、质检、入库或载入任务，请先终止任务"
        )

    cursor.execute(
        """SELECT publication_id FROM partition_publications
            WHERE dataset_id=%s AND status IN ('publishing','active','withdrawing')
            ORDER BY requested_at DESC LIMIT 1""",
        (dataset_id,),
    )
    publication = cursor.fetchone()
    if publication is not None:
        raise DatasetDeletionRejected(
            "publication",
            "数据集仍有未撤回的发布（publication_id=%s），请先撤回后再删除"
            % _row_value(publication, "publication_id"),
        )


def _collect_object_uris(connection: Any, plan: DeletionPlan) -> list[str]:
    """Generated partition objects of the dataset; loader-owned sources never."""
    uris: set[str] = set()
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT tile_uri AS object_uri FROM partition_tiles WHERE dataset_id=%s
               UNION SELECT value_ref_uri AS object_uri FROM partition_indexes WHERE dataset_id=%s
               UNION SELECT object_uri FROM partition_output_chunks WHERE dataset_id=%s""",
            (plan.dataset_id, plan.dataset_id, plan.dataset_id),
        )
        for row in cursor.fetchall():
            value = _row_value(row, "object_uri")
            if _owned_partition_object(value, plan.dataset_id):
                uris.add(str(value))
    uris.update(
        managed_object_uris(
            connection,
            dataset_id=plan.dataset_id,
            run_ids=plan.ids("ingest_job_ids"),
            output_versions=plan.ids("output_versions"),
        )
    )
    return sorted(uris)


def _owned_partition_object(value: Any, dataset_id: str) -> bool:
    """True only for generated outputs below this dataset's version prefix."""
    if not value:
        return False
    parsed = urlparse(unquote(str(value)))
    if parsed.scheme.lower() != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        return False
    # urlparse keeps the leading slash (``/partition/<dataset>/...``).
    return parsed.path.lstrip("/").startswith(partition_object_prefix(dataset_id))


def _ids(rows: Iterable[Any], field: str) -> list[str]:
    return _dedupe(str(_row_value(row, field)) for row in rows or () if _row_value(row, field))


def _row_value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return row.get(key)
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key)
    if isinstance(row, (list, tuple)):
        return row[0] if row else None
    return getattr(row, key, None)


def _dedupe(values: Iterable[Any]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if value))
