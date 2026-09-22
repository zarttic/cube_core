"""Unit tests for the batched dataset-deletion engine.

The engine replaced a single 790 s transaction per dataset deletion
(2026-09-19): the plan is built while the rows still exist, then executed as
small committed batches with per-step counts and timings.
"""

from __future__ import annotations

from typing import Any

import pytest

from cube_web.services.dataset_deletion import (
    DEFAULT_BATCH_SIZE,
    DatasetDeletionInterrupted,
    DatasetDeletionRejected,
    DeletionPlan,
    build_deletion_plan,
    execute_deletion_plan,
    partition_object_prefix,
)

DATASET_ID = "dataset-a"
PARTITION_TILE = f"s3://cube/partition/{DATASET_ID}/versions/ov-1/tiles/1.tif"
SOURCE_OBJECT = "s3://cube/cube/source/optocal/输入.tif"
RAW_ASSET_COPY = f"s3://cube/cube/raw/dataset={DATASET_ID}/sensor=s1/version=v1/a.tif"
_RS_TABLES = (
    "rs_raw_scene_asset",
    "rs_entity_tile_asset",
    "rs_product_asset",
    "rs_product_cell_fact",
    "rs_cube_cell_fact",
    "rs_carbon_observation_fact",
    "rs_ingest_job",
)


class _Cursor:
    def __init__(self, conn: "_Connection") -> None:
        self._conn = conn
        self._rows: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self._conn.executed.append((normalized, params))
        self._rows, self.rowcount = self._conn.answer(normalized, params)

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _Connection:
    """Table-driven fake: canned selector rows plus scripted delete counts."""

    def __init__(
        self,
        *,
        dataset_title: str | None = "标题",
        scene_ids: tuple[str, ...] = ("scene-1",),
        active: bool = False,
        publication: str | None = None,
        delete_counts: dict[str, list[int]] | None = None,
        tables: tuple[str, ...] = _RS_TABLES,
        object_uris: tuple[str, ...] = (PARTITION_TILE, SOURCE_OBJECT),
        partition_run_ids: tuple[str, ...] = (),
        draft_ids: tuple[str, ...] = (),
        scheduler_batch_ids: tuple[str, ...] = (),
    ) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self._dataset_title = dataset_title
        self._scene_ids = scene_ids
        self._active = active
        self._publication = publication
        self._delete_counts = {key: list(value) for key, value in (delete_counts or {}).items()}
        self._tables = tables
        self._object_uris = object_uris
        self._partition_run_ids = partition_run_ids
        self._draft_ids = draft_ids
        self._scheduler_batch_ids = scheduler_batch_ids
        self.closed = False

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True

    def answer(self, statement: str, params: tuple[Any, ...]) -> tuple[list[dict[str, Any]], int]:
        if statement.startswith("DELETE FROM"):
            table = statement.split()[2]
            counts = self._delete_counts.get(table)
            return [], (counts.pop(0) if counts else 1)
        if "FROM datasets WHERE dataset_id=%s FOR UPDATE" in statement:
            if self._dataset_title is None:
                return [], 1
            return [{"dataset_id": DATASET_ID, "dataset_title": self._dataset_title}], 1
        if "FROM scenes WHERE dataset_id=%s FOR UPDATE" in statement:
            return [{"scene_id": scene_id} for scene_id in self._scene_ids], 0
        if "batch_id FROM partition_datasets" in statement:
            return [{"batch_id": batch_id} for batch_id in self._scheduler_batch_ids], 0
        if "FROM partition_run_scenes WHERE dataset_id=%s FOR UPDATE" in statement:
            return [{"partition_run_id": run_id} for run_id in self._partition_run_ids], 0
        if "FROM ingest_runs WHERE dataset_id=%s FOR UPDATE" in statement:
            return [], 0
        if "FROM partition_drafts" in statement and "selection->'datasets'" in statement:
            return [
                {"draft_id": draft_id, "submitted_partition_run_id": None, "source_load_batch_ids": []}
                for draft_id in self._draft_ids
            ], 0
        if "jsonb_array_elements_text(scene.band_unit_ids)" in statement:
            return [{"output_version": "ov-1", "band_unit_id": "band-1"}], 0
        if "FROM partition_output_versions WHERE dataset_id=%s" in statement:
            return [{"output_version": "ov-1"}, {"output_version": "ov-2"}], 0
        if "FROM pg_class" in statement:
            return [{"relname": table} for table in self._tables], 0
        if "AS object_uri FROM partition_tiles" in statement:
            return [{"object_uri": uri} for uri in self._object_uris], 0
        if "raw_cog_uri AS object_uri FROM rs_raw_scene_asset" in statement:
            return [{"object_uri": RAW_ASSET_COPY}, {"object_uri": SOURCE_OBJECT}], 0
        if statement.startswith("SELECT EXISTS"):
            return [{"active": self._active}], 0
        if "FROM partition_publications" in statement:
            return ([{"publication_id": self._publication}] if self._publication else []), 0
        return [], 0

    def deletes(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [entry for entry in self.executed if entry[0].startswith("DELETE FROM")]

    def delete_batches(self, table: str) -> list[tuple[str, tuple[Any, ...]]]:
        return [entry for entry in self.deletes() if entry[0].split()[2] == table]


def _plan(connection: _Connection, **kwargs: Any) -> DeletionPlan:
    plan, _ = build_deletion_plan(
        connection, dataset_id=DATASET_ID, actor="admin", **kwargs
    )
    return plan


def test_plan_resolves_every_identifier_while_the_rows_still_exist() -> None:
    plan = _plan(_Connection())

    assert plan.dataset_id == DATASET_ID
    assert plan.dataset_title == "标题"
    assert plan.identifiers["scene_ids"] == ["scene-1"]
    assert plan.identifiers["output_versions"] == ["ov-1", "ov-2"]
    assert plan.identifiers["ingest_job_ids"], "the ingest job id must be derivable"
    assert plan.object_prefix == f"partition/{DATASET_ID}/"


def test_steps_are_ordered_for_foreign_keys_and_scope_rs_rows() -> None:
    plan = _plan(_Connection())
    names = [step.name for step in plan.steps]
    tables = [step.table for step in plan.steps]

    assert names[0].startswith("ingest."), "rs_* rows go first"
    assert tables.index("partition_indexes") < tables.index("partition_tiles"), (
        "tiles reference indexes; deleting tiles first would fire one check per row"
    )
    assert names[-1] == "dataset.datasets", "the dataset row is removed last"
    assert tables.index("partition_output_versions") < tables.index("datasets")


def test_plan_skips_rs_tables_that_do_not_exist_yet() -> None:
    plan = _plan(_Connection(tables=("rs_cube_cell_fact",)))

    assert [step.name for step in plan.steps if step.group == "ingest"] == ["ingest.rs_cube_cell_fact"]


def test_plan_rejects_a_missing_dataset() -> None:
    with pytest.raises(DatasetDeletionRejected) as excinfo:
        _plan(_Connection(dataset_title=None))

    assert excinfo.value.code == "not_found"


def test_plan_rejects_a_dataset_with_running_work_before_any_delete() -> None:
    connection = _Connection(active=True)

    with pytest.raises(DatasetDeletionRejected) as excinfo:
        _plan(connection)

    assert excinfo.value.code == "conflict"
    assert connection.deletes() == [], "guards must reject before any deletion"


def test_plan_rejects_a_dataset_with_a_live_publication() -> None:
    connection = _Connection(publication="pub-1")

    with pytest.raises(DatasetDeletionRejected) as excinfo:
        _plan(connection)

    assert excinfo.value.code == "publication"
    assert "pub-1" in str(excinfo.value)
    assert connection.deletes() == []


def test_each_batch_is_committed_and_counted() -> None:
    connection = _Connection(
        delete_counts={
            "partition_indexes": [DEFAULT_BATCH_SIZE, 5],
            "partition_tiles": [0],
        }
    )
    plan = _plan(connection)
    progress: list[tuple[str, int]] = []

    outcome = execute_deletion_plan(
        connection, plan, on_step=lambda step, total: progress.append((step.name, total))
    )

    assert outcome["deleted_rows"]["partition.indexes"] == DEFAULT_BATCH_SIZE + 5
    assert len(connection.delete_batches("partition_indexes")) == 2
    assert outcome["deleted_rows"]["partition.tiles"] == 0
    assert len(connection.delete_batches("partition_tiles")) == 1
    # Every batch is its own transaction, which is what keeps a large deletion
    # from starving every other session.
    assert connection.commits >= len(connection.deletes())
    assert progress[-1][0] == "dataset.datasets"
    steps = {step["name"]: step for step in outcome["steps"]}
    assert steps["partition.indexes"]["batches"] == 2
    assert steps["partition.indexes"]["seconds"] >= 0
    assert len(steps) == len(plan.steps)


def test_batch_size_can_be_overridden_per_run() -> None:
    connection = _Connection(delete_counts={"partition_indexes": [10, 0]})
    plan = _plan(connection)

    execute_deletion_plan(connection, plan, batch_size=10)

    statements = [statement for statement, _ in connection.delete_batches("partition_indexes")]
    assert all(" LIMIT 10)" in statement for statement in statements)


def test_replaying_a_plan_is_idempotent() -> None:
    connection = _Connection()
    plan = _plan(connection)

    first = execute_deletion_plan(connection, plan)
    second = execute_deletion_plan(connection, DeletionPlan.from_json(plan.to_json()))

    assert first["deleted_rows"] == second["deleted_rows"]
    assert second["deleted_total"] == sum(1 for _ in plan.steps)  # one fake row per step
    assert [step["name"] for step in second["steps"]] == [step.name for step in plan.steps]


def test_execution_stops_promptly_when_asked() -> None:
    connection = _Connection()
    plan = _plan(connection)

    with pytest.raises(DatasetDeletionInterrupted):
        execute_deletion_plan(connection, plan, should_stop=lambda: True)

    assert connection.deletes() == []


def test_plan_round_trips_through_json() -> None:
    plan = _plan(_Connection())

    restored = DeletionPlan.from_json(plan.to_json())

    assert restored == plan
    assert restored.steps[0].params == plan.steps[0].params


def test_sync_collection_keeps_generated_objects_and_drops_sources() -> None:
    connection = _Connection()
    plan, object_uris = build_deletion_plan(
        connection, dataset_id=DATASET_ID, actor="admin", collect_object_uris=True
    )

    assert plan.dataset_id == DATASET_ID
    assert PARTITION_TILE in object_uris
    assert RAW_ASSET_COPY in object_uris
    assert SOURCE_OBJECT not in object_uris


def test_object_prefix_rejects_unsafe_dataset_ids() -> None:
    for value in ("", "a/b", "..", "a\\b"):
        with pytest.raises(ValueError):
            partition_object_prefix(value)


def test_dataset_list_hides_datasets_with_an_unfinished_deletion() -> None:
    """The list query itself excludes queued/running deletions (not just the UI)."""
    from cube_web.services.dataset_management import ACTIVE_DELETION_FILTER

    assert "dataset_deletion_runs" in ACTIVE_DELETION_FILTER
    assert "deletion.status IN ('queued','running')" in ACTIVE_DELETION_FILTER
    assert "deletion.dataset_id = d.dataset_id" in ACTIVE_DELETION_FILTER


#: Foreign keys among the tables a deletion touches, read from the live schema
#: (2026-09-20).  child → parent: the child row must be deleted first, because
#: the executor commits after every batch and therefore cannot rely on the two
#: DEFERRABLE constraints on partition_datasets.
DELETION_FOREIGN_KEYS: tuple[tuple[str, str], ...] = (
    ("ingest_run_scenes", "ingest_runs"),
    ("ingest_run_scenes", "partition_runs"),
    ("ingest_run_scenes", "scenes"),
    ("ingest_runs", "datasets"),
    ("ingest_runs", "partition_runs"),
    ("load_batch_scenes", "load_batches"),
    ("load_batch_scenes", "scenes"),
    ("load_batch_sources", "datasets"),
    ("load_batch_sources", "load_batches"),
    ("partition_assets", "partition_batches"),
    ("partition_data_unit_grid_status", "datasets"),
    ("partition_data_unit_grid_status", "partition_runs"),
    ("partition_data_unit_grid_status", "scenes"),
    ("partition_dataset_assets", "partition_datasets"),
    ("partition_dataset_bands", "partition_dataset_assets"),
    ("partition_datasets", "partition_batches"),
    # DEFERRABLE on the live schema: only safe because partition_datasets is
    # deleted before the rows it points at.
    ("partition_datasets", "partition_output_versions"),
    ("partition_datasets", "partition_quality_runs"),
    ("partition_domain_outbox", "partition_output_versions"),
    ("partition_drafts", "partition_runs"),
    ("partition_grid_cells", "partition_output_versions"),
    ("partition_indexes", "partition_dataset_bands"),
    ("partition_indexes", "partition_output_versions"),
    ("partition_indexes", "partition_tiles"),
    ("partition_job_attempts", "partition_batches"),
    ("partition_logical_staging_rows", "partition_output_versions"),
    ("partition_output_chunks", "partition_output_versions"),
    ("partition_output_versions", "partition_job_attempts"),
    ("partition_publication_targets", "partition_dataset_bands"),
    ("partition_publication_targets", "partition_publications"),
    ("partition_publications", "partition_quality_runs"),
    ("partition_quality_errors", "partition_quality_runs"),
    ("partition_quality_results", "partition_quality_runs"),
    ("partition_quality_runs", "partition_output_versions"),
    ("partition_quality_warn_approvals", "partition_quality_runs"),
    ("partition_run_scenes", "datasets"),
    ("partition_run_scenes", "load_batches"),
    ("partition_run_scenes", "partition_runs"),
    ("partition_run_scenes", "scenes"),
    ("partition_tiles", "partition_dataset_bands"),
    ("partition_tiles", "partition_output_versions"),
    ("scene_assets", "scenes"),
    ("scene_bands", "scene_assets"),
    ("scene_dataset_audit", "datasets"),
    ("scene_dataset_audit", "scenes"),
    ("scenes", "datasets"),
)


def test_step_order_satisfies_every_foreign_key_between_deleted_tables() -> None:
    """A batched deletion commits per batch, so order must respect the FK graph.

    Regression for the 2026-09-20 production failure: deleting
    partition_quality_runs before partition_datasets raised
    ``partition_datasets_current_quality_run_fkey`` (a DEFERRABLE constraint that
    the legacy single-transaction delete only checked at its final commit).
    """
    plan = _plan(_Connection(scheduler_batch_ids=("batch-1",)))
    position = {step.table: index for index, step in enumerate(plan.steps)}

    checked = [
        (child, parent)
        for child, parent in DELETION_FOREIGN_KEYS
        if child in position and parent in position
    ]
    violations = [
        f"{child} (step {position[child]}) must precede {parent} (step {position[parent]})"
        for child, parent in checked
        if position[child] > position[parent]
    ]

    assert len(checked) >= 25, f"the FK check must not be vacuous, only {len(checked)} pairs applied"
    assert violations == [], "deletion order violates foreign keys: " + "; ".join(violations)


def test_quality_runs_are_deleted_after_the_dataset_pointer_row() -> None:
    plan = _plan(_Connection())
    tables = [step.table for step in plan.steps]

    assert tables.index("partition_datasets") < tables.index("partition_quality_runs")
    assert tables.index("partition_datasets") < tables.index("partition_output_versions")
    assert tables.index("partition_indexes") < tables.index("partition_tiles")
    assert tables.index("partition_publication_targets") < tables.index("partition_datasets")


def test_step_order_satisfies_foreign_keys_when_runs_and_drafts_exist() -> None:
    """Same invariant with the draft/run steps present (the graph is larger)."""
    connection = _Connection(
        partition_run_ids=("run-1", "run-2"),
        draft_ids=("draft-1",),
        delete_counts={"partition_runs": [0]},
    )
    plan = _plan(connection)
    position = {step.table: index for index, step in enumerate(plan.steps)}
    assert "partition_runs" in position and "partition_drafts" in position

    violations = [
        f"{child} (step {position[child]}) must precede {parent} (step {position[parent]})"
        for child, parent in DELETION_FOREIGN_KEYS
        if child in position and parent in position and position[child] > position[parent]
    ]

    assert violations == [], "deletion order violates foreign keys: " + "; ".join(violations)


def test_a_stored_plan_can_be_refreshed_to_the_current_engine() -> None:
    """Queued jobs must not replay a stale step order (2026-09-20 FK incident)."""
    from cube_web.services.dataset_deletion import (
        PLAN_VERSION,
        DeletionStep,
        refresh_deletion_steps,
    )

    legacy = DeletionPlan(
        dataset_id=DATASET_ID,
        dataset_title="标题",
        requested_by="admin",
        identifiers={"scene_ids": ["scene-1"], "output_versions": ["ov-1"]},
        object_prefix=f"partition/{DATASET_ID}/",
        # Old order: quality_runs before partition_datasets (FK violation).
        steps=(
            DeletionStep(name="partition.quality_runs", table="partition_quality_runs", predicate="dataset_id=%s",
                         params=(DATASET_ID,)),
            DeletionStep(name="partition.datasets", table="partition_datasets", predicate="dataset_id=%s",
                         params=(DATASET_ID,)),
        ),
        plan_version=1,
    )

    refreshed = refresh_deletion_steps(_Connection(), legacy)
    tables = [step.table for step in refreshed.steps]

    assert refreshed.plan_version == PLAN_VERSION
    assert refreshed.identifiers == legacy.identifiers, "resolved identifiers must survive the refresh"
    assert refreshed.object_prefix == legacy.object_prefix
    assert tables.index("partition_datasets") < tables.index("partition_quality_runs")
    assert "partition_quality_errors" in tables


def test_plan_is_validated_before_anything_is_deleted() -> None:
    """A broken step must fail up front, not after thousands of rows are gone."""
    from cube_web.services.dataset_deletion import validate_deletion_plan

    connection = _Connection()
    plan = _plan(connection)
    connection.executed.clear()

    assert validate_deletion_plan(connection, plan) == []
    explained = [statement for statement, _ in connection.executed]
    assert len(explained) == len(plan.steps)
    assert all(statement.startswith("EXPLAIN DELETE FROM") for statement in explained)
    assert connection.deletes() == [], "validation must not execute a deletion"


def test_broken_step_aborts_before_the_first_delete() -> None:
    # The fake needs run ids so the plan actually contains the step we break.
    connection = _Connection(partition_run_ids=("run-1",), delete_counts={"partition_runs": [0]})
    plan = _plan(connection)

    original = connection.answer

    def answer(statement: str, params: tuple[Any, ...]):
        if statement.startswith("EXPLAIN") and "partition_runs" in statement:
            raise RuntimeError('column "dataset_id" does not exist')
        return original(statement, params)

    connection.answer = answer  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="pre-flight validation"):
        execute_deletion_plan(connection, plan)

    assert connection.deletes() == []


#: Fingerprint of the step list (name|table|predicate, in order).  Any change to
#: the deletion plan must update this AND bump PLAN_VERSION: a queued job is only
#: re-materialised when the stored version differs, so a silent step change keeps
#: replaying stale SQL (that shipped two production bugs on 2026-09-20).
EXPECTED_STEP_FINGERPRINT = "753810cba08f65d5"


def _step_fingerprint(plan: DeletionPlan) -> str:
    import hashlib

    payload = "\n".join(
        f"{step.name}|{step.table}|{step.predicate}|{step.statement}" for step in plan.steps
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def test_plan_version_tracks_the_step_definitions() -> None:
    plan = _plan(
        _Connection(
            partition_run_ids=("run-1",),
            draft_ids=("draft-1",),
            scheduler_batch_ids=("batch-1",),
            delete_counts={"partition_runs": [0]},
        )
    )

    assert _step_fingerprint(plan) == EXPECTED_STEP_FINGERPRINT, (
        "the deletion step list changed: bump PLAN_VERSION and update "
        "EXPECTED_STEP_FINGERPRINT together"
    )


def test_a_failed_statement_leaves_the_connection_usable() -> None:
    """The worker must be able to record the failure (rollback, not abort)."""

    class _Tx:
        def __init__(self) -> None:
            self.rollbacks = 0

        def rollback(self) -> None:
            self.rollbacks += 1

    connection = _Connection()
    tracker = _Tx()
    connection.rollback = tracker.rollback  # type: ignore[attr-defined]
    plan = _plan(connection)
    original = connection.answer

    def answer(statement: str, params: tuple[Any, ...]):
        if statement.startswith("DELETE FROM partition_indexes"):
            raise RuntimeError("boom")
        return original(statement, params)

    connection.answer = answer  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="boom"):
        execute_deletion_plan(connection, plan, validate=False)

    assert tracker.rollbacks >= 1, "a failed statement must be rolled back"


def test_a_deleted_dataset_is_pruned_from_every_scheduler_batch() -> None:
    """A batch can hold several datasets: delete the dataset, or the batch if empty.

    Regression for the leftover `partition_batches` rows whose stored payload kept
    listing a dataset that had already been deleted (2026-09-20).
    """
    plan = _plan(_Connection(scheduler_batch_ids=("batch-1", "batch-2")))
    steps = {step.name: step for step in plan.steps}

    prune = steps["scheduler.partition_batch_payload"]
    assert prune.table == "partition_batches"
    assert "normalized_payload" in prune.statement
    assert "'{datasets}'" in prune.statement and "'{dataset_partitions}'" in prune.statement
    assert "entry->>'dataset_id' <> %s" in prune.statement
    # OpenGauss 7.0 has neither jsonb_agg nor jsonb_build_object: the plan must not
    # contain them or the whole step fails on the server.
    assert "jsonb_agg" not in prune.statement and "jsonb_build_object" not in prune.statement
    assert "json_agg(entry)::jsonb" in prune.statement
    assert prune.params == (DATASET_ID, DATASET_ID, ["batch-1", "batch-2"], DATASET_ID)

    delete = steps["scheduler.partition_batches"]
    assert "COALESCE(jsonb_array_length(normalized_payload->'datasets'), 0) = 0" in delete.predicate, (
        "a batch that still holds another dataset must survive"
    )
    assert "NOT EXISTS (SELECT 1 FROM partition_datasets dataset" in delete.predicate

    # Pruning must happen before the batch row is removed.
    names = [step.name for step in plan.steps]
    assert names.index("scheduler.partition_batch_payload") < names.index("scheduler.partition_batches")


def test_statements_steps_are_explained_not_run_as_deletes() -> None:
    from cube_web.services.dataset_deletion import validate_deletion_plan

    connection = _Connection(scheduler_batch_ids=("batch-1",))
    plan = _plan(connection)
    connection.executed.clear()

    assert validate_deletion_plan(connection, plan) == []
    explained = [statement for statement, _ in connection.executed]
    prune = next(item for item in explained if "normalized_payload" in item)
    assert prune.startswith("EXPLAIN UPDATE partition_batches"), prune
    assert connection.deletes() == []


def test_plan_sql_avoids_json_functions_opengauss_lacks() -> None:
    """Real-schema lesson: OpenGauss has no jsonb_agg / jsonb_build_object."""
    plan = _plan(
        _Connection(
            partition_run_ids=("run-1",),
            draft_ids=("draft-1",),
            scheduler_batch_ids=("batch-1",),
            delete_counts={"partition_runs": [0]},
        )
    )

    offenders = [
        f"{step.name}: {name}"
        for step in plan.steps
        for name in ("jsonb_agg", "jsonb_build_object", "jsonb_build_array")
        if name in f"{step.predicate} {step.statement}"
    ]

    assert offenders == [], "unsupported JSON functions in the plan: " + "; ".join(offenders)
