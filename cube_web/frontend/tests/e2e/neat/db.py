#!/usr/bin/env python3.11
"""Read-only DB helper for E2E verification, plus ledger-scoped cleanup and perf- residue purge.

Usage:
  db.py counts datasets scenes partition_runs ...          # absolute counts (baseline comparison)
  db.py run <partition_run_id>                             # per-run row counts
  db.py ledger-counts <ledger.json>                        # counts restricted to ledger run ids
  db.py pairs <ledger.json>                                # output pairs exclusively owned by ledger runs
  db.py cleanup <ledger.json> --confirm                    # delete ledger runs + perf- residue
  db.py cleanup-ledger <ledger.json> --confirm [--no-objects] [--dataset-restore <snapshot.json>]  # delete ONLY ledger rows + their MinIO objects
  db.py snapshot-datasets <out.json> <dataset_id>...     # capture shared partition_datasets rows before a run
  db.py verify <ledger.json>                               # post-cleanup verification
"""
from __future__ import annotations

import json
import sys

import psycopg

from cube_split.runtime_config import postgres_dsn

BASELINE_TABLES = [
    "datasets", "scenes", "partition_grid_cells", "partition_indexes", "partition_tiles",
    "rs_cube_cell_fact", "partition_runs", "ingest_runs", "partition_quality_runs",
    "load_batches", "load_batch_scenes",
]

# table -> SQL predicate selecting perf- residue (children before parents for FK safety)
PERF_WHERE = [
    ("ingest_run_scenes", "(ingest_run_id LIKE 'perf-%' OR partition_run_id LIKE 'perf-%')"),
    ("ingest_runs", "(ingest_run_id LIKE 'perf-%' OR partition_run_id LIKE 'perf-%')"),
    ("partition_data_unit_grid_status", "partition_run_id LIKE 'perf-%'"),
    ("rs_cube_cell_fact", "run_id LIKE 'perf-%'"),
    ("rs_ingest_job", "job_id LIKE 'perf-%'"),
    ("rs_raw_scene_asset", "run_id LIKE 'perf-%'"),
    ("partition_quality_runs", "partition_run_id LIKE 'perf-%'"),
    ("partition_run_scenes", "partition_run_id LIKE 'perf-%'"),
    ("partition_runs", "partition_run_id LIKE 'perf-%'"),
]
PERF_TABLES = [(table, where) for table, where in PERF_WHERE]

# Output tables have no run column: rows are owned by (dataset_id, output_version) pairs
# recorded on partition_run_scenes. Perf pairs are those used only by perf- runs.
OUTPUT_TABLES = ["partition_output_versions", "partition_indexes", "partition_tiles", "partition_grid_cells"]
PERF_PAIRS_SQL = """
    SELECT DISTINCT rsc.dataset_id, rsc.output_version
    FROM partition_run_scenes rsc
    WHERE rsc.partition_run_id LIKE 'perf-%%' AND rsc.output_version IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 FROM partition_run_scenes other
            WHERE other.dataset_id = rsc.dataset_id AND other.output_version = rsc.output_version
              AND other.partition_run_id NOT LIKE 'perf-%%')
"""
LEDGER_PAIRS_SQL = """
    SELECT DISTINCT rsc.dataset_id, rsc.output_version
    FROM partition_run_scenes rsc
    WHERE rsc.partition_run_id = ANY(%s) AND rsc.output_version IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 FROM partition_run_scenes other
            WHERE other.dataset_id = rsc.dataset_id AND other.output_version = rsc.output_version
              AND NOT (other.partition_run_id = ANY(%s)))
"""
LEDGER_RUN_TABLES = [
    ("ingest_run_scenes", "partition_run_id"),
    ("ingest_runs", "partition_run_id"),
    ("partition_run_scenes", "partition_run_id"),
    ("partition_data_unit_grid_status", "partition_run_id"),
    ("rs_cube_cell_fact", "run_id"),
    ("rs_ingest_job", "job_id"),
    ("rs_raw_scene_asset", "job_id"),
    ("partition_output_versions", "partition_run_id"),
    ("partition_indexes", "partition_run_id"),
    ("partition_tiles", "partition_run_id"),
    ("partition_grid_cells", "partition_run_id"),
    ("partition_quality_runs", "partition_run_id"),
    ("partition_runs", "partition_run_id"),
]


def out(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, default=str))


def counts(tables: list[str]) -> dict:
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        result = {}
        for table in tables:
            cur.execute(f'SELECT count(*) FROM "{table}"')
            result[table] = cur.fetchone()[0]
        return result


def run_counts(run_id: str) -> dict:
    result = {"partition_run_id": run_id}
    columns = {
        "partition_runs": "partition_run_id",
        "partition_run_scenes": "partition_run_id",
        "partition_data_unit_grid_status": "partition_run_id",
        "partition_quality_runs": "partition_run_id",
        "rs_cube_cell_fact": "run_id",
        "rs_ingest_job": "job_id",
        "rs_raw_scene_asset": "run_id",
    }
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for table, _where in PERF_TABLES:
            try:
                if table in ("ingest_runs", "ingest_run_scenes"):
                    cur.execute(
                        f'SELECT count(*) FROM "{table}" WHERE "partition_run_id" = %s OR "ingest_run_id" = %s',
                        (run_id, run_id),
                    )
                else:
                    cur.execute(f'SELECT count(*) FROM "{table}" WHERE "{columns[table]}" = %s', (run_id,))
                result[table] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[table] = "n/a"
        for table in OUTPUT_TABLES:
            try:
                cur.execute(
                    f'SELECT count(*) FROM "{table}" t JOIN ('
                    '  SELECT dataset_id, output_version FROM partition_run_scenes WHERE partition_run_id = %s'
                    ') p ON p.dataset_id = t.dataset_id AND p.output_version = t.output_version',
                    (run_id,),
                )
                result[table] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[table] = "n/a"
        try:
            cur.execute('SELECT status FROM "partition_runs" WHERE "partition_run_id" = %s', (run_id,))
            row = cur.fetchone()
            result["status"] = row[0] if row else None
        except psycopg.errors.UndefinedColumn:
            conn.rollback()
            result["status"] = "n/a"
    return result


def load_ledger(path: str) -> dict:
    with open(path, encoding="utf8") as handle:
        return json.load(handle)


def ledger_run_ids(ledger: dict) -> list[str]:
    ids = [entry["run_id"] for entry in ledger.get("runs", []) if entry.get("run_id")]
    return sorted(set(ids))


def ledger_counts(path: str) -> dict:
    ledger = load_ledger(path)
    ids = ledger_run_ids(ledger)
    result = {"ledger_run_ids": ids}
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for table, column in LEDGER_RUN_TABLES:
            if not ids:
                result[table] = 0
                continue
            try:
                if table in ("ingest_runs", "ingest_run_scenes"):
                    cur.execute(
                        f'SELECT count(*) FROM "{table}" WHERE "partition_run_id" = ANY(%s) OR "ingest_run_id" = ANY(%s)',
                        (ids, ids),
                    )
                else:
                    cur.execute(f'SELECT count(*) FROM "{table}" WHERE "{column}" = ANY(%s)', (ids,))
                result[table] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[table] = "n/a"
        if ids:
            try:
                cur.execute('SELECT count(*) FROM "partition_runs" WHERE "partition_run_id" = ANY(%s)', (ids,))
                result["ledger_ids_present_in_partition_runs"] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result["ledger_ids_present_in_partition_runs"] = "n/a"
    return result


PAIR_DELETE_ORDER = [
    # children before parents; partition_indexes references partition_tiles,
    # publications reference quality runs.
    # partition_datasets is deliberately absent: it is a shared one-row-per-dataset projection
    # that must be restored (see dataset_restore), never deleted.
    "partition_quality_errors", "partition_quality_warn_approvals", "partition_quality_results",
    "partition_publication_targets", "partition_publications",
    "partition_indexes", "partition_tiles", "partition_grid_cells",
    "partition_output_chunks", "partition_logical_staging_rows", "partition_domain_outbox",
    "partition_quality_runs",
    "partition_output_versions",
]
BATCH_DELETE_ORDER = ["partition_job_attempts", "partition_assets", "partition_batches"]


def dataset_row(dataset_id: str) -> dict | None:
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM partition_datasets WHERE dataset_id = %s", (dataset_id,))
        row = cur.fetchone()
        if row is None:
            return None
        columns = [description[0] for description in cur.description]
        return {name: value for name, value in zip(columns, row)}


def snapshot_datasets(path: str, dataset_ids: list[str]) -> dict:
    rows = [row for dataset_id in dataset_ids if (row := dataset_row(dataset_id))]
    with open(path, "w", encoding="utf8") as handle:
        json.dump({"datasets": rows}, handle, ensure_ascii=False, indent=1, default=str)
        handle.write("\n")
    return {"path": path, "datasets": len(rows)}


def load_dataset_restore(path: str) -> list[dict]:
    with open(path, encoding="utf8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return payload
    return list(payload.get("datasets") or [])


def ledger_pairs(path: str) -> list[dict]:
    """(dataset_id, output_version) pairs owned exclusively by ledger runs, with MinIO prefix."""
    ledger = load_ledger(path)
    ids = ledger_run_ids(ledger)
    if not ids:
        return []
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT rsc.dataset_id, rsc.output_version, ov.object_prefix
            FROM partition_run_scenes rsc
            LEFT JOIN partition_output_versions ov
              ON ov.dataset_id = rsc.dataset_id AND ov.output_version = rsc.output_version
            WHERE rsc.partition_run_id = ANY(%s) AND rsc.output_version IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1 FROM partition_run_scenes other
                    WHERE other.dataset_id = rsc.dataset_id
                      AND other.output_version = rsc.output_version
                      AND NOT (other.partition_run_id = ANY(%s)))
            """,
            (ids, ids),
        )
        return [
            {"dataset_id": row[0], "output_version": row[1], "object_prefix": row[2]}
            for row in cur.fetchall()
        ]


def delete_pair_objects(pairs: list[dict]) -> dict:
    prefixes = []
    for pair in pairs:
        prefix = pair.get("object_prefix") or f"partition/{pair['dataset_id']}/versions/{pair['output_version']}/"
        prefixes.append(prefix if prefix.endswith("/") else f"{prefix}/")
    if not prefixes:
        return {"objects_removed": 0, "prefixes": []}
    from minio import Minio
    from cube_split.runtime_config import minio_settings

    settings = minio_settings()
    client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
    removed = 0
    for prefix in prefixes:
        for obj in client.list_objects(settings.bucket, prefix=prefix, recursive=True):
            client.remove_object(settings.bucket, obj.object_name)
            removed += 1
    return {"objects_removed": removed, "prefixes": prefixes}


def prefix_run_counts() -> dict:
    result: dict[str, int] = {}
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for pattern in ("neat-%", "e2e-%", "perf-%"):
            cur.execute('SELECT count(*) FROM "partition_runs" WHERE "partition_run_id" LIKE %s', (pattern,))
            result[pattern] = cur.fetchone()[0]
    return result


def cleanup_ledger(path: str, *, remove_objects: bool = True, dataset_restore: str | None = None) -> dict:
    """Delete exactly the ledger runs' rows (FK-safe) plus their MinIO objects. No perf- purge.

    ``dataset_restore`` is a snapshot json (see snapshot_datasets) applied to the shared
    partition_datasets projection rows before their ledger-owned outputs are deleted.
    """
    ledger = load_ledger(path)
    ids = ledger_run_ids(ledger)
    pairs = ledger_pairs(path)
    objects = None
    if remove_objects and pairs:
        try:
            objects = delete_pair_objects(pairs)
        except Exception as exc:  # object store is best-effort; DB cleanup still proceeds
            objects = {"error": f"{type(exc).__name__}: {exc}"[:400]}
    restore_rows = load_dataset_restore(dataset_restore) if dataset_restore else []
    counts_before = counts(BASELINE_TABLES)
    restored: list[str] = []
    removed: dict[str, int] = {}
    with psycopg.connect(postgres_dsn(), autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'partition_datasets'"
        )
        allowed = {row[0] for row in cur.fetchall()}
        for row in restore_rows:
            dataset_id = row.get("dataset_id")
            columns = {
                key: (json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value)
                for key, value in row.items()
                if key != "dataset_id" and key in allowed
            }
            if not dataset_id or not columns:
                continue
            assignments = ", ".join(f'"{name}" = %s' for name in columns)
            cur.execute(
                f'UPDATE "partition_datasets" SET {assignments} WHERE "dataset_id" = %s',
                (*columns.values(), dataset_id),
            )
            restored.append(dataset_id)
        for table in PAIR_DELETE_ORDER:
            total = 0
            for pair in pairs:
                rows = safe_delete(
                    cur,
                    f'DELETE FROM "{table}" WHERE "dataset_id" = %s AND "output_version" = %s',
                    (pair["dataset_id"], pair["output_version"]),
                )
                if rows > 0:
                    total += rows
            removed[f"pairs:{table}"] = total
        for table, column in LEDGER_RUN_TABLES:
            if table in PAIR_DELETE_ORDER:
                continue
            removed[f"ledger:{table}"] = (
                safe_delete(cur, f'DELETE FROM "{table}" WHERE "{column}" = ANY(%s)', (ids,)) if ids else 0
            )
        for table in BATCH_DELETE_ORDER:
            removed[f"batch:{table}"] = (
                safe_delete(cur, f'DELETE FROM "{table}" WHERE "batch_id" = ANY(%s)', (ids,)) if ids else 0
            )
        conn.commit()
    return {
        "ledger_run_ids": ids,
        "pairs": pairs,
        "objects": objects,
        "datasets_restored": restored,
        "removed": removed,
        "counts_before": counts_before,
        "counts_after": counts(BASELINE_TABLES),
        "ledger_after": ledger_counts(path),
        "prefix_runs_after": prefix_run_counts(),
    }


def verify_ledger(path: str) -> dict:
    return {
        "ledger_run_ids": ledger_run_ids(load_ledger(path)),
        "ledger_after": ledger_counts(path),
        "remaining_pairs": ledger_pairs(path),
        "prefix_runs": prefix_run_counts(),
        "counts": counts(BASELINE_TABLES),
    }


def perf_counts() -> dict:
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        result = {}
        for table, where in PERF_WHERE:
            try:
                cur.execute(f'SELECT count(*) FROM "{table}" WHERE {where}')
                result[f"{table}"] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[f"{table}"] = "n/a"
        for table in OUTPUT_TABLES:
            try:
                cur.execute(
                    f'SELECT count(*) FROM "{table}" t JOIN ({PERF_PAIRS_SQL}) p '
                    'ON p.dataset_id = t.dataset_id AND p.output_version = t.output_version'
                )
                result[f"{table}.perf_pairs"] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[f"{table}.perf_pairs"] = "n/a"
        return result


def protected_counts() -> dict:
    """Counts of rows that are NOT perf residue and NOT ledger-owned (must be unchanged by cleanup)."""
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        result: dict[str, object] = {}
        for table, where in PERF_WHERE:
            try:
                cur.execute(f'SELECT count(*) FROM "{table}" WHERE NOT ({where})')
                result[table] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[table] = "n/a"
        for table in OUTPUT_TABLES:
            try:
                cur.execute(
                    f'SELECT count(*) FROM "{table}" t WHERE NOT EXISTS ('
                    f'  SELECT 1 FROM ({PERF_PAIRS_SQL}) p '
                    '  WHERE p.dataset_id = t.dataset_id AND p.output_version = t.output_version)'
                )
                result[table] = cur.fetchone()[0]
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                conn.rollback()
                result[table] = "n/a"
        return result


def safe_delete(cur, sql: str, params=None) -> int:
    """DELETE that tolerates missing tables/columns without aborting the transaction."""
    cur.execute("SAVEPOINT cleanup_step")
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        removed = cur.rowcount
        cur.execute("RELEASE SAVEPOINT cleanup_step")
        return removed
    except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
        cur.execute("ROLLBACK TO SAVEPOINT cleanup_step")
        return -1


def cleanup(path: str) -> dict:
    ledger = load_ledger(path)
    ids = ledger_run_ids(ledger)
    removed: dict[str, int] = {}
    protected_before = protected_counts()
    with psycopg.connect(postgres_dsn(), autocommit=False) as conn, conn.cursor() as cur:
        # 1) ledger-scoped rows (children before parents)
        for table, column in LEDGER_RUN_TABLES:
            if not ids:
                removed[f"ledger:{table}"] = 0
                continue
            removed[f"ledger:{table}"] = safe_delete(
                cur, f'DELETE FROM "{table}" WHERE "{column}" = ANY(%s)', (ids,)
            )
        # 2) perf- prefix-keyed residue (children first per PERF_WHERE order)
        for table, where in PERF_WHERE:
            removed[f"perf:{table}"] = safe_delete(cur, f'DELETE FROM "{table}" WHERE {where}')
        # 3) output tables owned exclusively by perf/ledger runs
        for label, sql, params in (("perf", PERF_PAIRS_SQL, ()), ("ledger", LEDGER_PAIRS_SQL, (ids, ids))):
            for table in OUTPUT_TABLES:
                removed[f"{label}-pairs:{table}"] = safe_delete(
                    cur,
                    f'DELETE FROM "{table}" t WHERE EXISTS ('
                    f'  SELECT 1 FROM ({sql}) p '
                    '  WHERE p.dataset_id = t.dataset_id AND p.output_version = t.output_version)',
                    params,
                )
        conn.commit()
    return {
        "ledger_run_ids": ids,
        "removed": removed,
        "protected_before": protected_before,
        "protected_after": protected_counts(),
        "perf_after": perf_counts(),
        "ledger_after": ledger_counts(path),
    }


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    command = argv[0]
    if command == "counts":
        out(counts(argv[1:] or BASELINE_TABLES))
    elif command == "baseline":
        out(counts(BASELINE_TABLES))
    elif command == "run":
        out(run_counts(argv[1]))
    elif command == "ledger-counts":
        out(ledger_counts(argv[1]))
    elif command == "perf-counts":
        out(perf_counts())
    elif command == "cleanup":
        if "--confirm" not in argv:
            print("refusing: pass --confirm")
            return 3
        out(cleanup(argv[1]))
    elif command == "cleanup-ledger":
        if "--confirm" not in argv:
            print("refusing: pass --confirm")
            return 3
        restore_path = None
        if "--dataset-restore" in argv:
            restore_path = argv[argv.index("--dataset-restore") + 1]
        out(cleanup_ledger(argv[1], remove_objects="--no-objects" not in argv, dataset_restore=restore_path))
    elif command == "snapshot-datasets":
        out(snapshot_datasets(argv[1], argv[2:]))
    elif command == "pairs":
        out(ledger_pairs(argv[1]))
    elif command == "verify":
        out(verify_ledger(argv[1]))
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
