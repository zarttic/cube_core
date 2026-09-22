"""Physical deletion of one load batch.

The ARD loader calls ``POST /v1/partition/load-batches/{id}/delete`` first and
falls back to ``/archive`` on 404; archiving is deprecated because it never
reclaimed any data.  This module implements the physical path.

Semantics (deliberately conservative — a batch deletion must never destroy data
that another batch still owns):

* A batch is deleted as a whole dataset only when every dataset it contributed
  to is referenced by no other load batch **and** the batch is not itself a
  ``dataset_reload`` batch (a reload re-runs an existing dataset; deleting its
  bookkeeping must not delete the dataset).
* Otherwise only this batch's own rows go away: its ``load_batch_scenes`` link
  rows, its ``load_batch_sources`` rows, ``load_batch_sources`` rows of *other*
  batches that pointed at this one as their reload source (bookkeeping), and
  finally the ``load_batches`` row itself.  Scenes and outputs stay, because
  outputs are versioned per dataset and cannot be removed per scene with the
  current keys.
* ``dry_run`` returns the full plan without writing anything.

The dataset deletion itself is delegated to ``DatasetManagementService`` so the
batch path reuses the already-verified dataset guards, ``partition_*``/``rs_*``
cleanup and object recycling.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

# A batch in these states still has work in flight and must not be deleted.
ACTIVE_BATCH_STATUSES = ("pending", "running")


class LoadBatchNotFound(LookupError):
    """No ``load_batches`` row for the requested id (HTTP 404)."""


class LoadBatchDeletionConflict(RuntimeError):
    """The batch is not deletable right now (HTTP 409)."""


def delete_load_batch(
    connection: Any,
    *,
    load_batch_id: str,
    actor: str,
    reason: str,
    dataset_admin: Any,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete one load batch; see the module docstring for the exact scope."""
    plan = _plan_deletion(connection, load_batch_id=load_batch_id, dry_run=dry_run)
    if dry_run:
        plan["dry_run"] = True
        return plan

    dataset_results: list[dict[str, Any]] = []
    for dataset_id in plan["deletable_dataset_ids"]:
        try:
            dataset_results.append(
                {
                    "dataset_id": dataset_id,
                    "result": dataset_admin.delete_dataset(dataset_id, actor=actor),
                }
            )
        except Exception as exc:  # dataset guards (active tasks/publication) or concurrent removal
            raise LoadBatchDeletionConflict(
                f"数据集删除失败（dataset_id={dataset_id}）: {type(exc).__name__}: {exc}"
            ) from exc

    deleted: dict[str, int] = {}
    with connection.cursor() as cur:
        # Bookkeeping rows of reload batches that sourced from this batch must go
        # first, otherwise the FK blocks removing the load_batches row.
        cur.execute("DELETE FROM load_batch_sources WHERE source_load_batch_id = %s", (load_batch_id,))
        deleted["referencing_load_batch_sources"] = max(int(cur.rowcount), 0)
        cur.execute("DELETE FROM load_batch_sources WHERE load_batch_id = %s", (load_batch_id,))
        deleted["load_batch_sources"] = max(int(cur.rowcount), 0)
        cur.execute("DELETE FROM load_batch_scenes WHERE load_batch_id = %s", (load_batch_id,))
        deleted["load_batch_scenes"] = max(int(cur.rowcount), 0)
        cur.execute("DELETE FROM load_batches WHERE load_batch_id = %s", (load_batch_id,))
        deleted["load_batches"] = max(int(cur.rowcount), 0)
        # Dataset deletion already removes the load-batch rows it owns, so a zero
        # rowcount here is expected; what matters is that no row is left behind.
        cur.execute("SELECT 1 FROM load_batches WHERE load_batch_id = %s", (load_batch_id,))
        if cur.fetchone() is not None:
            raise LoadBatchDeletionConflict(f"批次行仍存在，删除未生效：{load_batch_id}")

    result = {
        **{key: value for key, value in plan.items() if key != "dry_run"},
        "dry_run": False,
        "deleted": deleted,
        "dataset_results": dataset_results,
        "reason": reason,
        "actor": actor,
    }
    # Durable trail without a schema change: one structured log record per deletion.
    logger.info("load_batch.delete %s", json.dumps(result, ensure_ascii=False, default=str))
    return result


def _plan_deletion(connection: Any, *, load_batch_id: str, dry_run: bool) -> dict[str, Any]:
    with connection.cursor(row_factory=dict_row) as cur:
        # Deliberately *no* FOR UPDATE: the dataset deletion below runs on its own
        # connection/transaction, so holding a row lock here would make the two
        # transactions wait on each other (observed as a request timeout in the
        # cross-system acceptance test).  The final existence check replaces the lock.
        cur.execute(
            "SELECT status, source_type FROM load_batches WHERE load_batch_id = %s",
            (load_batch_id,),
        )
        batch = cur.fetchone()
        if batch is None:
            raise LoadBatchNotFound(load_batch_id)
        status = str(batch["status"] or "")
        source_type = str(batch["source_type"] or "subsystem_import")
        if status in ACTIVE_BATCH_STATUSES:
            raise LoadBatchDeletionConflict(f"批次仍有进行中的任务（status={status}），请先终止任务后再删除")

        cur.execute(
            """SELECT DISTINCT s.scene_id, s.dataset_id
                 FROM load_batch_scenes lbs
                 JOIN scenes s ON s.scene_id = lbs.scene_id
                WHERE lbs.load_batch_id = %s""",
            (load_batch_id,),
        )
        rows = cur.fetchall()
        scene_ids = sorted({str(row["scene_id"]) for row in rows})
        dataset_ids = sorted({str(row["dataset_id"]) for row in rows})

        shared_dataset_ids: set[str] = set()
        if dataset_ids:
            cur.execute(
                """SELECT s.dataset_id, count(DISTINCT lbs.load_batch_id) AS batch_count
                     FROM load_batch_scenes lbs
                     JOIN scenes s ON s.scene_id = lbs.scene_id
                    WHERE s.dataset_id = ANY(%s::text[]) AND lbs.load_batch_id <> %s
                    GROUP BY s.dataset_id""",
                (dataset_ids, load_batch_id),
            )
            shared_dataset_ids = {str(row["dataset_id"]) for row in cur.fetchall()}

    deletable = [dataset_id for dataset_id in dataset_ids if dataset_id not in shared_dataset_ids]
    retained = [dataset_id for dataset_id in dataset_ids if dataset_id in shared_dataset_ids]
    if source_type == "dataset_reload":
        # Reload bookkeeping never owns the dataset it re-runs.
        retained = sorted({*retained, *deletable})
        deletable = []

    return {
        "load_batch_id": load_batch_id,
        "status": status,
        "source_type": source_type,
        "scene_ids": scene_ids,
        "dataset_ids": dataset_ids,
        "deletable_dataset_ids": deletable,
        "retained_dataset_ids": retained,
        "dry_run": dry_run,
    }
