"""Dataset-scoped cleanup for the ``rs_*`` ingest tables.

``cube_web`` owns dataset deletion but not this schema, so it resolves the
dataset's own identifiers and calls :func:`delete_managed_dataset_rows` from
inside its deletion transaction.  The ownership keys were verified against the
live schema:

* ``rs_raw_scene_asset`` / ``rs_entity_tile_asset`` / ``rs_product_asset`` /
  ``rs_product_cell_fact`` store the owning dataset id in their ``dataset``
  column, so they delete by that column.
* ``rs_cube_cell_fact`` / ``rs_carbon_observation_fact`` have no dataset column.
  Managed ingest keys them by ``cube_version`` (the dataset's own
  ``output_version``) and by ``run_id`` (:func:`make_ingest_job_id`); both values
  are derived from the dataset id, so a version or run id can never belong to two
  datasets.
* ``rs_ingest_job`` rows use the same ingest job id as ``run_id``.

Rows written by a hand-run CLI ingest with an unrelated version
(``--cube-version v1``) belong to no dataset row and stay behind by design: they
cannot be attributed without guessing.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Iterable, Sequence
from urllib.parse import unquote, urlparse

# Tables that carry the owning dataset id in their ``dataset`` column.
DATASET_COLUMN_TABLES: tuple[str, ...] = (
    "rs_raw_scene_asset",
    "rs_entity_tile_asset",
    "rs_product_asset",
    "rs_product_cell_fact",
)

# Tables without a dataset column: ownership resolves through cube_version / run_id.
RUN_SCOPED_TABLES: tuple[str, ...] = ("rs_cube_cell_fact", "rs_carbon_observation_fact")

INGEST_JOB_TABLE = "rs_ingest_job"

# Only ingest-created copies are reported back for object-store cleanup.  Window
# references and loader-owned source objects (``s3://user-N/datas/...``, the
# ``.../source/...`` prefixes) must survive: nothing under a source prefix may
# ever be deleted, even if a row points at it.
OBJECT_URI_COLUMNS: tuple[tuple[str, str], ...] = (
    ("rs_raw_scene_asset", "raw_cog_uri"),
    ("rs_entity_tile_asset", "tile_uri"),
    ("rs_product_asset", "cog_uri"),
)

# Hard guard: these path fragments mark loader/user-owned source data.
SOURCE_PATH_MARKERS: tuple[str, ...] = ("/datas/", "/source/", "/raw_data/", "/nfs/", "/shared_delivery")
# Buckets that carry user uploads (the ingestion source lives here).
SOURCE_BUCKET_PREFIXES: tuple[str, ...] = ("user-", "shared-")


@dataclass(frozen=True)
class ManagedTableCleanup:
    """One table-scoped delete owned by a dataset.

    ``predicate`` is a SQL condition over ``table``'s own columns; ``params``
    binds its placeholders.  Both are built from module constants only, never
    from caller input, so they are safe to interpolate into a statement.
    """

    table: str
    predicate: str
    params: tuple[Any, ...]
    object_uri_column: str | None = None


def managed_table_cleanups(
    dataset_id: str,
    *,
    run_ids: Sequence[str] = (),
    output_versions: Sequence[str] = (),
) -> tuple[ManagedTableCleanup, ...]:
    """Return the ``rs_*`` deletes one dataset owns, in foreign-key-free order.

    Tables whose ownership key is absent (no ingest job ids, no output
    versions) are omitted, so a call is always safe to make: it simply has
    fewer statements to run.  Callers that batch the deletes (dataset deletion
    job) and callers that delete in one transaction both build their statements
    from this single description.
    """
    run_id_values = [str(value) for value in run_ids if str(value)]
    version_values = [str(value) for value in output_versions if str(value)]
    uri_columns = dict(OBJECT_URI_COLUMNS)
    cleanups: list[ManagedTableCleanup] = []

    for table in DATASET_COLUMN_TABLES:
        cleanups.append(
            ManagedTableCleanup(
                table=table,
                predicate="dataset = %s",
                params=(dataset_id,),
                object_uri_column=uri_columns.get(table),
            )
        )

    clause, params = _version_or_run_clause(run_id_values, version_values)
    if clause:
        cleanups.append(ManagedTableCleanup(table="rs_cube_cell_fact", predicate=clause, params=params))

    if run_id_values:
        cleanups.append(
            ManagedTableCleanup(
                table="rs_carbon_observation_fact",
                predicate="run_id = ANY(%s::text[])",
                params=(run_id_values,),
            )
        )
        cleanups.append(
            ManagedTableCleanup(
                table=INGEST_JOB_TABLE,
                predicate="job_id = ANY(%s::text[])",
                params=(run_id_values,),
            )
        )

    return tuple(cleanups)


def existing_managed_tables(conn: Any) -> set[str]:
    """Tables from the ``rs_*`` contract that exist in the current schema."""
    with conn.cursor() as cur:
        return _existing_tables(cur, [*DATASET_COLUMN_TABLES, *RUN_SCOPED_TABLES, INGEST_JOB_TABLE])


def delete_table_batch(
    conn: Any,
    *,
    table: str,
    predicate: str,
    params: Sequence[Any],
    batch_size: int,
) -> int:
    """Delete at most ``batch_size`` matching rows and return the row count.

    Used by the dataset-deletion job to keep every transaction small: the
    caller commits between batches, so a deletion of hundreds of thousands of
    rows never becomes one long transaction that starves the database.
    ``table``/``predicate`` come from :func:`managed_table_cleanups` (or from
    ``cube_web``'s own step table), never from request input.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    statement = (
        f"DELETE FROM {table} WHERE ctid IN "
        f"(SELECT ctid FROM {table} WHERE {predicate} LIMIT {int(batch_size)})"
    )
    with conn.cursor() as cur:
        cur.execute(statement, tuple(params))
        return max(int(cur.rowcount), 0)


def make_ingest_job_id(dataset_id: str, output_version: str, band_unit_id: str) -> str:
    """Managed ingest job id, and therefore ``rs_cube_cell_fact.run_id``.

    This is the single source of truth for the id: it must stay byte-identical to
    the value stored at ingest time, otherwise dataset cleanup cannot find the
    rows it owns.
    """
    identity = f"{dataset_id}\0{output_version}\0{band_unit_id}"
    return f"ingest-{sha256(identity.encode()).hexdigest()[:24]}"


def managed_object_uris(
    conn: Any,
    *,
    dataset_id: str,
    run_ids: Sequence[str] = (),
    output_versions: Sequence[str] = (),
) -> list[str]:
    """Collect the ingest-created objects this dataset owns, without deleting.

    Only copies that pass :func:`_owned_ingest_object` are returned; loader and
    user source objects are never included, so a caller can hand the result to
    an object store knowing originals are out of scope.
    """
    uris: set[str] = set()
    with conn.cursor() as cur:
        known = _existing_tables(cur, [spec.table for spec in managed_table_cleanups(dataset_id)])
        for cleanup in managed_table_cleanups(
            dataset_id, run_ids=run_ids, output_versions=output_versions
        ):
            if cleanup.object_uri_column is None or cleanup.table not in known:
                continue
            cur.execute(
                f"SELECT {cleanup.object_uri_column} AS object_uri FROM {cleanup.table} WHERE {cleanup.predicate}",
                cleanup.params,
            )
            uris.update(
                value
                for value in (_owned_ingest_object(_first_column(row), dataset_id) for row in cur.fetchall())
                if value
            )
    return sorted(uris)


def delete_managed_dataset_rows(
    conn: Any,
    *,
    dataset_id: str,
    run_ids: Sequence[str] = (),
    output_versions: Sequence[str] = (),
) -> dict[str, Any]:
    """Delete every ``rs_*`` row owned by one dataset.

    Runs inside the caller's transaction: it issues no ``COMMIT`` and no
    ``ROLLBACK``.  Tables that do not exist yet are skipped rather than failing,
    so a deployment that never ingested can still delete a dataset.

    Returns ``{"deleted_rows": {table: count}, "deleted_total": int,
    "object_uris": [...]}`` where ``object_uris`` are the ingest-created objects
    that became unreachable and should be removed from the object store.
    """
    deleted_rows: dict[str, int] = {}
    object_uris: set[str] = set()

    with conn.cursor() as cur:
        known = _existing_tables(
            cur, [*DATASET_COLUMN_TABLES, *RUN_SCOPED_TABLES, INGEST_JOB_TABLE]
        )

        for cleanup in managed_table_cleanups(
            dataset_id, run_ids=run_ids, output_versions=output_versions
        ):
            if cleanup.table not in known:
                continue
            if cleanup.object_uri_column is not None:
                cur.execute(
                    f"SELECT {cleanup.object_uri_column} AS object_uri FROM {cleanup.table} WHERE {cleanup.predicate}",
                    cleanup.params,
                )
                object_uris.update(
                    value
                    for value in (_owned_ingest_object(_first_column(row), dataset_id) for row in cur.fetchall())
                    if value
                )
            deleted_rows[cleanup.table] = _delete(
                cur, f"DELETE FROM {cleanup.table} WHERE {cleanup.predicate}", cleanup.params
            )

    return {
        "deleted_rows": deleted_rows,
        "deleted_total": sum(deleted_rows.values()),
        "object_uris": sorted(object_uris),
    }


def _delete(cur: Any, statement: str, params: tuple[Any, ...]) -> int:
    cur.execute(statement, params)
    return max(int(cur.rowcount), 0)


def _version_or_run_clause(run_ids: Sequence[str], versions: Sequence[str]) -> tuple[str, tuple[Any, ...]]:
    clauses: list[str] = []
    params: list[Any] = []
    if run_ids:
        clauses.append("run_id = ANY(%s::text[])")
        params.append(list(run_ids))
    if versions:
        clauses.append("cube_version = ANY(%s::text[])")
        params.append(list(versions))
    return " OR ".join(clauses), tuple(params)


def _existing_tables(cur: Any, names: Iterable[str]) -> set[str]:
    candidates = sorted({str(name) for name in names})
    if not candidates:
        return set()
    cur.execute(
        """SELECT c.relname
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ANY(%s::text[])
              AND c.relkind IN ('r', 'p')
              AND n.nspname = ANY(current_schemas(false))""",
        (candidates,),
    )
    return {str(_first_column(row)) for row in cur.fetchall()}


def _owned_ingest_object(value: Any, dataset_id: str) -> str:
    """Return the object uri when it is an ingest copy this dataset owns.

    Refuses anything that looks like loader/user source data (see
    ``SOURCE_PATH_MARKERS`` / ``SOURCE_BUCKET_PREFIXES``) so a deletion can never
    remove the original data; only copies under the dataset's own prefixes pass.
    """
    if not value:
        return ""
    parsed = urlparse(unquote(str(value)))
    if parsed.scheme.lower() != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        return ""
    bucket = parsed.netloc.lower()
    path = parsed.path
    lowered = path.lower()
    if bucket.startswith(SOURCE_BUCKET_PREFIXES) or any(marker in lowered for marker in SOURCE_PATH_MARKERS):
        return ""
    if f"/dataset={dataset_id}/" in path or path.startswith(f"/partition/{dataset_id}/versions/"):
        return str(value)
    return ""


def _first_column(row: Any) -> Any:
    if hasattr(row, "values"):
        return next(iter(row.values()), None)
    return row[0] if row else None
