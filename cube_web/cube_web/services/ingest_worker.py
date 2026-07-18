from __future__ import annotations

from hashlib import sha256
from typing import Any, Callable

from cube_split import runtime_config
from cube_split.ingest.managed_output_ingest import ingest_managed_output
from psycopg.rows import dict_row

from cube_web.services.ingest_repository import OpenGaussIngestRepository


def process_queued_ingest_scenes(
    *,
    limit: int = 10,
    repository: Any | None = None,
    verifier: Callable[[Any, dict[str, str]], None] | None = None,
    executor: Callable[..., Any] | None = None,
) -> int:
    """Ingest quality-approved managed outputs, then complete their Scenes."""
    repo = repository or OpenGaussIngestRepository(runtime_config.postgres_dsn())
    verify = verifier or _verify_current_partition_output
    execute = executor or ingest_managed_output
    claimed = repo.claim_queued_outputs(limit=limit)
    completed = 0
    for group in claimed:
        items = tuple(group["items"])
        claim_token = str(group.get("claim_token") or "")
        production_execution = executor is None
        try:
            verified = [verify(repo, item) for item in items]
            output_dataset_ids = {str(value) for value in verified if value}
            if len(output_dataset_ids) != 1:
                raise RuntimeError("ingest Scenes do not resolve to one managed output Dataset")
            output_dataset_id = output_dataset_ids.pop()
            identity = f"{group['dataset_id']}\0{group['output_version']}"
            ingest_job_id = f"ingest-{sha256(identity.encode()).hexdigest()[:24]}"
            scene_ids = tuple(sorted({item["scene_id"] for item in items}))
            if executor is None:
                with repo.pool.connection() as connection:
                    execute(
                        connection,
                        dataset_id=group["dataset_id"],
                        output_dataset_id=output_dataset_id,
                        output_version=group["output_version"],
                        ingest_job_id=ingest_job_id,
                        scene_ids=scene_ids,
                        before_commit=lambda conn: repo.complete_claimed_output(conn, items, claim_token),
                    )
            else:
                execute(
                    repo,
                    dataset_id=group["dataset_id"],
                    output_dataset_id=output_dataset_id,
                    output_version=group["output_version"],
                    ingest_job_id=ingest_job_id,
                    scene_ids=scene_ids,
                )
            if not production_execution:
                for item in items:
                    repo.complete_scene(item["ingest_run_id"], item["scene_id"])
            completed += len(items)
        except Exception as exc:
            if production_execution and claim_token:
                repo.fail_claimed_output(items, claim_token, str(exc)[:2000])
            else:
                for item in items:
                    try:
                        repo.fail_scene(item["ingest_run_id"], item["scene_id"], str(exc)[:2000])
                    except Exception:
                        continue
    return completed


def _verify_current_partition_output(repository: OpenGaussIngestRepository, item: dict[str, str]) -> str:
    with repository.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT d.current_output_version,
                   d.dataset_id AS output_dataset_id,
                   prs.status AS scene_partition_status,prs.output_version AS scene_output_version
            FROM datasets d
            JOIN partition_run_scenes prs ON prs.dataset_id=d.dataset_id AND prs.scene_id=%s
            WHERE d.dataset_id=%s AND prs.output_version=%s
            ORDER BY prs.updated_at DESC LIMIT 1
            """,
            (item["scene_id"], item["dataset_id"], item["output_version"]),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("partition Scene output is missing")
        if str(row["current_output_version"] or "") != item["output_version"]:
            raise RuntimeError("ingest output is no longer the Dataset current version")
        if row["scene_partition_status"] != "completed" or str(row["scene_output_version"] or "") != item["output_version"]:
            raise RuntimeError("partition Scene output is not completed")
        cur.execute(
            """
            SELECT status FROM partition_output_versions
            WHERE dataset_id=%s AND output_version=%s
            """,
            (row["output_dataset_id"], item["output_version"]),
        )
        output = cur.fetchone()
        if output is None or output["status"] != "completed":
            raise RuntimeError("partition output version is not completed")
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM partition_indexes WHERE dataset_id=%s AND output_version=%s) AS index_count,
              (SELECT count(*) FROM partition_tiles WHERE dataset_id=%s AND output_version=%s) AS tile_count,
              (SELECT count(*) FROM partition_grid_cells WHERE dataset_id=%s AND output_version=%s) AS grid_cell_count
            """,
            (
                row["output_dataset_id"], item["output_version"],
                row["output_dataset_id"], item["output_version"],
                row["output_dataset_id"], item["output_version"],
            ),
        )
        counts = cur.fetchone()
        if counts is None or int(counts["index_count"] or 0) == 0 or int(counts["grid_cell_count"] or 0) == 0:
            raise RuntimeError("partition output has no managed indexes or grid cells")
        return str(row["output_dataset_id"])
