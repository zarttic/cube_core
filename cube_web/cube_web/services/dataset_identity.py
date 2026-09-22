"""Dataset identity resolution for load-schema imports.

Two modes, chosen per deployment and per product:

``legacy`` (default)
    Trust the ``dataset_id`` in the payload.  Because the ARD loader derives it
    from the load batch (``ard-<data_type>-<load_batch_id>``), every re-import of
    a product creates another dataset copy.

``name``
    Resolve the dataset by ``(dataset_title, data_type)`` first: a product that is
    loaded again appends into the dataset it already has, and the batch is kept
    only as lineage (``load_batch_sources``).

``CUBE_WEB_IMPORT_IDENTITY_PRODUCTS`` narrows ``name`` mode to a ``|``-separated
whitelist of dataset titles, so a single product can be piloted before the whole
deployment switches over.  Switching back to ``legacy`` restores the previous
behaviour exactly (rows already written are not rewritten).

Legacy data may already hold several **live** copies of one product (they were
never archived).  An import must never fail because of that, so the resolver picks
deterministically -- the copy with the most recent partition output wins, else the
most recently created one -- and records the ambiguity in the audit trail instead
of raising.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from cube_split import runtime_config

DATASET_IDENTITY_ENV = "CUBE_WEB_IMPORT_DATASET_IDENTITY"
IDENTITY_PRODUCTS_ENV = "CUBE_WEB_IMPORT_IDENTITY_PRODUCTS"

LEGACY_MODE = "legacy"
NAME_MODE = "name"
VALID_MODES = (LEGACY_MODE, NAME_MODE)


def row_value(row: Any, index: int, key: str) -> Any:
    """Read a column from either a positional (plain) or a mapping (dict_row) row.

    ``db_pool`` connects without a ``row_factory``, so write paths receive plain
    tuples while ``_read`` helpers receive dict rows; shared helpers must accept both.
    """
    if isinstance(row, Mapping):
        return row[key]
    return row[index]


def row_mapping(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    """Normalize one row into a dict using the SELECT's column order."""
    if isinstance(row, Mapping):
        return {column: row[column] for column in columns if column in row}
    return dict(zip(columns, row))


def identity_mode() -> str:
    raw = str(runtime_config.env_text(DATASET_IDENTITY_ENV) or "").strip().lower()
    return raw if raw in VALID_MODES else LEGACY_MODE


def identity_product_whitelist() -> tuple[str, ...]:
    raw = str(runtime_config.env_text(IDENTITY_PRODUCTS_ENV) or "")
    return tuple(title.strip() for title in raw.split("|") if title.strip())


def uses_name_identity(dataset: Mapping[str, Any]) -> bool:
    """Whether this dataset should be resolved by name instead of by payload id."""
    if identity_mode() != NAME_MODE:
        return False
    whitelist = identity_product_whitelist()
    if not whitelist:
        return True
    return str(dataset.get("dataset_title") or "").strip() in whitelist


def match_dataset_by_name(
    cursor: Any, *, dataset_title: str, data_type: str
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Resolve the product identity to one live dataset.

    Returns ``(chosen, candidates)``.  ``candidates`` holds every live dataset with
    this identity, ordered by preference, so the caller can audit an ambiguous
    situation (legacy duplicates that were never archived).  The chosen one is the
    dataset with the most recent partition output -- i.e. the copy the deployment
    actually works on -- falling back to the most recently created one.
    """
    cursor.execute(
        """SELECT d.dataset_id, d.dataset_code, d.dataset_title, d.data_type,
                  d.created_at,
                  (SELECT max(v.created_at) FROM partition_output_versions v
                    WHERE v.dataset_id = d.dataset_id) AS last_output_at
             FROM datasets d
            WHERE d.dataset_title = %s AND d.data_type = %s AND d.status <> 'archived'
            ORDER BY last_output_at DESC NULLS LAST, d.created_at DESC""",
        (dataset_title, data_type),
    )
    columns = ("dataset_id", "dataset_code", "dataset_title", "data_type", "created_at", "last_output_at")
    candidates = [row_mapping(row, columns) for row in (cursor.fetchall() or [])]
    if not candidates:
        return None, []
    return candidates[0], candidates


def record_identity_audit(
    cursor: Any,
    *,
    dataset_id: str,
    load_batch_id: str,
    action: str,
    previous_dataset_id: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    """Append one identity decision to ``datasets.attributes.identity_audit``.

    JSONB instead of a new table so the change stays DDL-free; the entry records
    who was resolved onto whom and from which batch.
    """
    entry = {
        "at": datetime.now(UTC).isoformat(),
        "action": action,
        "load_batch_id": load_batch_id,
        "previous_dataset_id": previous_dataset_id,
        **(dict(details) if details else {}),
    }
    cursor.execute(
        """UPDATE datasets
              SET attributes = jsonb_set(
                    COALESCE(attributes, '{}'::jsonb),
                    '{identity_audit}',
                    COALESCE(attributes -> 'identity_audit', '[]'::jsonb) || %s::jsonb,
                    true
                  ),
                  updated_at = now()
            WHERE dataset_id = %s""",
        (_json(entry), dataset_id),
    )


def record_scene_content_refresh(
    cursor: Any,
    *,
    scene_id: str,
    load_batch_id: str,
    previous_checksum: str,
    incoming_checksum: str,
    refreshed_in_place: bool,
) -> None:
    """Append one content refresh to ``scenes.attributes.content_history``."""
    entry = {
        "at": datetime.now(UTC).isoformat(),
        "action": "content_refresh",
        "load_batch_id": load_batch_id,
        "previous_checksum": previous_checksum,
        "checksum": incoming_checksum,
        "assets_refreshed_in_place": refreshed_in_place,
    }
    cursor.execute(
        """UPDATE scenes
              SET attributes = jsonb_set(
                    COALESCE(attributes, '{}'::jsonb),
                    '{content_history}',
                    COALESCE(attributes -> 'content_history', '[]'::jsonb) || %s::jsonb,
                    true
                  ),
                  updated_at = now()
            WHERE scene_id = %s""",
        (_json(entry), scene_id),
    )


def _json(entry: Mapping[str, Any]) -> str:
    """jsonb_set receives an array literal so the audit chain keeps appending."""
    return json.dumps([entry], ensure_ascii=False)
