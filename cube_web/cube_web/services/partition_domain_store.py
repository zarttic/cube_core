"""Persistence boundary for the M2 versioned partition domain.

The in-memory implementation is deliberately complete rather than a loose mock:
it is used by workflow tests and mirrors ownership, ordering, idempotency, and
outbox semantics of the SQL implementation.  The SQL store keeps the same
behavioural surface while exposing a transaction context for M3 operations.
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from threading import RLock
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:  # pragma: no cover
    from cube_web.services.partition_contracts import (
        DatasetInput,
        PartitionDatasetResult,
        StrictPartitionRequest,
    )

SortOrder = Literal["asc", "desc"]
SCHEMA_VERSION = "2026-07-16-m2-mixed-carbon-v1"

_DATASET_SORTS = {
    "updated_at": "updated_at",
    "created_at": "created_at",
    "dataset_code": "dataset_code",
    "partition_completed_at": "partition_completed_at",
    "quality_status": "quality_status",
}
_DETAIL_SORTS = {
    "assets": {"source_asset_id": "source_asset_id", "created_at": "created_at"},
    "bands": {"display_order": "display_order", "band_code": "band_code"},
    "tiles": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "indexes": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "grid_cells": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "publications": {"requested_at": "requested_at", "activated_at": "activated_at", "status": "status"},
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_value(item) for item in value]
    if isinstance(value, list):
        return [_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _value(item) for key, item in value.items()}
    return value


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _asset_source_uri(asset: Any) -> str:
    """Return the canonical source URI for either a COG or raw carbon asset."""
    source_uri = _field(asset, "source_uri") or _field(asset, "cog_uri")
    if source_uri is None:
        raise ValueError("source asset is missing its canonical source URI")
    return str(source_uri)


def _output_version(dataset_id: str, task_id: str) -> str:
    try:
        from cube_web.services.partition_contracts import make_output_version

        return make_output_version(dataset_id, task_id)
    except ImportError:  # allows this slice to be tested before contract cherry-pick
        return hashlib.sha256(f"{dataset_id}\0{task_id}".encode()).hexdigest()[:32]


def _validate_page(limit: int, offset: int, sort_by: str, sort_order: str, allowed: dict[str, str]) -> None:
    if not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if sort_by not in allowed:
        raise ValueError(f"invalid sort_by: {sort_by}")
    if sort_order not in {"asc", "desc"}:
        raise ValueError(f"invalid sort_order: {sort_order}")


class PartitionDomainStore:
    """Dataset-result persistence boundary."""

    def ensure_schema(self) -> None:
        raise NotImplementedError

    def start_output(self, request: "StrictPartitionRequest", dataset: "DatasetInput", task_id: str) -> str:
        raise NotImplementedError

    def complete_output(self, result: "PartitionDatasetResult") -> dict[str, Any]:
        raise NotImplementedError

    def fail_output(self, dataset_id: str, output_version: str, *, error_code: str, error_message: str) -> None:
        raise NotImplementedError

    def resolve_output_version(self, dataset_id: str, output_version: str | None = None) -> str:
        raise NotImplementedError

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def list_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: SortOrder,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
    ) -> int:
        raise NotImplementedError

    def get_output_version(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def list_assets(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_assets(self, dataset_id: str, output_version: str | None = None) -> int:
        raise NotImplementedError

    def list_bands(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_bands(self, dataset_id: str, output_version: str | None = None) -> int:
        raise NotImplementedError

    def list_tiles(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_tiles(self, dataset_id: str, output_version: str | None = None) -> int:
        raise NotImplementedError

    def list_indexes(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_indexes(self, dataset_id: str, output_version: str | None = None) -> int:
        raise NotImplementedError

    def list_grid_cells(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_grid_cells(self, dataset_id: str, output_version: str | None = None) -> int:
        raise NotImplementedError

    def list_publications(self, dataset_id: str, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]:
        raise NotImplementedError

    def count_publications(self, dataset_id: str) -> int:
        raise NotImplementedError

    def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool:
        raise NotImplementedError

    def get_output_cleanup_state(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def claim_outbox(self, worker_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        raise NotImplementedError

    def acknowledge_outbox(self, event_id: str) -> None:
        raise NotImplementedError

    def retry_outbox(self, event_id: str, error: str, *, available_at: str) -> None:
        raise NotImplementedError


class InMemoryPartitionDomainStore(PartitionDomainStore):
    """Behavioral test double with OpenGauss parity."""

    def __init__(self) -> None:
        self.datasets: dict[str, dict[str, Any]] = {}
        self.assets: dict[tuple[str, str], dict[str, Any]] = {}
        self.bands: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.outputs: dict[tuple[str, str], dict[str, Any]] = {}
        self.tiles: dict[str, dict[str, Any]] = {}
        self.indexes: dict[str, dict[str, Any]] = {}
        self.grid_cells: dict[str, dict[str, Any]] = {}
        self.publications: list[dict[str, Any]] = []
        self.outbox: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, dict[str, Any]] = {}
        self.schema_version = SCHEMA_VERSION
        self.fail_on_output_id: str | None = None
        self._lock = RLock()

    def ensure_schema(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise RuntimeError(f"partition domain schema version {self.schema_version!r} does not match {SCHEMA_VERSION!r}")

    def start_output(self, request: "StrictPartitionRequest", dataset: "DatasetInput", task_id: str) -> str:
        self.ensure_schema()
        dataset_id = str(_field(dataset, "dataset_id"))
        version = _output_version(dataset_id, task_id)
        with self._lock:
            existing = self.outputs.get((dataset_id, version))
            if existing is not None:
                if existing["task_id"] != task_id:
                    raise ValueError("output version collides with a different task")
                return version
            now = _now()
            req_grid = int(_field(request, "requested_grid_level", 0))
            grid_type = str(_field(request, "grid_type", "geohash"))
            method = str(_field(request, "partition_method", "logical"))
            self._upsert_dataset(request, dataset, now)
            self.outputs[(dataset_id, version)] = {
                "dataset_id": dataset_id,
                "output_version": version,
                "task_id": task_id,
                "grid_type": grid_type,
                "requested_grid_level": req_grid,
                "partition_method": method,
                "object_prefix": f"partition/{dataset_id}/versions/{version}/",
                "status": "staging",
                "tile_count": 0,
                "index_count": 0,
                "grid_cell_count": 0,
                "counts": {},
                "created_at": now,
                "completed_at": None,
                "failed_at": None,
                "error_code": None,
                "error_message": None,
            }
            self.attempts.setdefault(task_id, {"task_id": task_id, "dataset_id": dataset_id, "status": "running"})
            return version

    def _upsert_dataset(self, request: Any, dataset: Any, now: str) -> dict[str, Any]:
        dataset_id = str(_field(dataset, "dataset_id"))
        existing = self.datasets.get(dataset_id)
        if existing is not None:
            if existing.get("dataset_code") != _field(dataset, "dataset_code"):
                raise ValueError("dataset identity conflict")
            return existing
        row = {
            "dataset_id": dataset_id,
            "dataset_code": str(_field(dataset, "dataset_code")),
            "dataset_title": str(_field(dataset, "dataset_title")),
            "data_type": _field(dataset, "data_type"),
            "product_type": _field(dataset, "product_type"),
            "batch_id": _field(request, "batch_id"),
            "grid_type": _field(request, "grid_type"),
            "requested_grid_level": _field(request, "requested_grid_level"),
            "partition_method": _field(request, "partition_method"),
            "cover_mode": _field(request, "cover_mode", "intersect"),
            "partition_status": "running",
            "current_output_version": None,
            "quality_status": "pending",
            "current_quality_run_id": None,
            "quality_sequence": 0,
            "quality_error_count": 0,
            "quality_warning_count": 0,
            "partition_completed_at": None,
            "attributes": _value(_field(dataset, "attributes", {})),
            "created_at": now,
            "updated_at": now,
        }
        self.datasets[dataset_id] = row
        for asset in _field(dataset, "assets", ()):
            aid = str(_field(asset, "source_asset_id"))
            self.assets[(dataset_id, aid)] = {
                "dataset_id": dataset_id,
                "source_asset_id": aid,
                "cog_uri": None if _field(asset, "cog_uri") is None else str(_field(asset, "cog_uri")),
                "source_uri": _asset_source_uri(asset),
                "source_kind": str(_field(asset, "source_kind", "cog")),
                "source_format": str(_field(asset, "source_format", "cog")),
                "checksum": _field(asset, "checksum"),
                "bbox": _value(_field(asset, "bbox")),
                "crs": _field(asset, "crs"),
                "time_start": _field(asset, "time_start"),
                "time_end": _field(asset, "time_end"),
                "attributes": _value(_field(asset, "attributes", {})),
                "created_at": now,
            }
        for band in _field(dataset, "bands", ()):
            key = (dataset_id, str(_field(band, "source_asset_id")), str(_field(band, "band_code")))
            self.bands[key] = {
                "dataset_id": dataset_id,
                "source_asset_id": key[1],
                "band_code": key[2],
                "band_name": _field(band, "band_name"),
                "band_type": _field(band, "band_type"),
                "unit": _field(band, "unit"),
                "display_order": _field(band, "display_order", 0),
                "attributes": _value(_field(band, "attributes", {})),
                "created_at": now,
            }
        return row

    def complete_output(self, result: "PartitionDatasetResult") -> dict[str, Any]:
        self.ensure_schema()
        dataset_id = str(_field(result, "dataset_id"))
        version = str(_field(result, "output_version"))
        task_id = str(_field(result, "task_id"))
        with self._lock:
            snapshot = {key: copy.deepcopy(value) for key, value in self.__dict__.items() if key != "_lock"}
            try:
                output = self.outputs.get((dataset_id, version))
                if output is None:
                    raise ValueError("output version has not been started")
                if output["status"] == "completed":
                    return copy.deepcopy(output)
                attempt = self.attempts.get(task_id, {})
                if attempt.get("status") in {"cancelled", "cancel_requested"}:
                    raise RuntimeError("partition attempt was cancelled")
                if output["task_id"] != task_id:
                    raise ValueError("output version task mismatch")
                rows = {
                    "tiles": _field(result, "tiles", ()),
                    "indexes": _field(result, "indexes", ()),
                    "grid_cells": _field(result, "grid_cells", ()),
                }
                for noun, target in (("tiles", self.tiles), ("indexes", self.indexes), ("grid_cells", self.grid_cells)):
                    for raw in rows[noun]:
                        row = _value(raw)
                        oid = str(row.get("output_id") or "")
                        if not oid:
                            raise ValueError(f"{noun} row is missing output_id")
                        if self.fail_on_output_id == oid:
                            raise RuntimeError("injected detail failure")
                        row.update({"dataset_id": dataset_id, "output_version": version, "created_at": row.get("created_at") or _now()})
                        existing = target.get(oid)
                        if existing is not None and existing != row:
                            raise ValueError(f"{noun} identity conflict")
                        target[oid] = row
                for noun, count_key in (("tiles", "tile_count"), ("indexes", "index_count"), ("grid_cells", "grid_cell_count")):
                    output[count_key] = len(
                        [
                            r
                            for r in getattr(self, noun).values()
                            if r.get("dataset_id") == dataset_id and r.get("output_version") == version
                        ]
                    )
                output["status"] = "completed"
                output["completed_at"] = _now()
                output["counts"] = {
                    "tiles": output["tile_count"],
                    "indexes": output["index_count"],
                    "grid_cells": output["grid_cell_count"],
                }
                dataset = self.datasets[dataset_id]
                old = dataset.get("current_output_version")
                if (
                    old
                    and old != version
                    and (dataset_id, old) in self.outputs
                    and self.outputs[(dataset_id, old)]["status"] == "completed"
                ):
                    self.outputs[(dataset_id, old)]["status"] = "superseded"
                dataset.update(
                    {
                        "current_output_version": version,
                        "partition_status": "completed",
                        "partition_completed_at": output["completed_at"],
                        "quality_status": "pending",
                        "current_quality_run_id": None,
                        "quality_error_count": 0,
                        "quality_warning_count": 0,
                        "updated_at": _now(),
                    }
                )
                event_key = f"{dataset_id}:{version}:output-version.completed"
                self.outbox.setdefault(
                    event_key,
                    {
                        "event_id": event_key,
                        "dataset_id": dataset_id,
                        "output_version": version,
                        "event_type": "output-version.completed",
                        "payload": {
                            "schema_version": SCHEMA_VERSION,
                            "event_type": "output-version.completed",
                            "dataset_id": dataset_id,
                            "output_version": version,
                            "task_id": task_id,
                            "grid_type": _field(result, "grid_type"),
                            "requested_grid_level": _field(result, "requested_grid_level"),
                            "partition_method": _field(result, "partition_method"),
                            "counts": output["counts"],
                        },
                        "status": "pending",
                        "attempt_count": 0,
                        "available_at": _now(),
                        "claimed_at": None,
                        "claimed_by": None,
                        "last_error": None,
                    },
                )
                return copy.deepcopy(output)
            except Exception:
                for key, value in snapshot.items():
                    if key != "_lock":
                        setattr(self, key, value)
                raise

    def fail_output(self, dataset_id: str, output_version: str, *, error_code: str, error_message: str) -> None:
        self.ensure_schema()
        output = self.outputs.get((dataset_id, output_version))
        if output is None:
            raise KeyError(output_version)
        if output["status"] == "completed":
            return
        output.update({"status": "failed", "error_code": error_code, "error_message": error_message, "failed_at": _now()})
        if self.datasets.get(dataset_id, {}).get("current_output_version") != output_version:
            self.datasets.get(dataset_id, {}).update({"partition_status": "failed", "updated_at": _now()})

    def resolve_output_version(self, dataset_id: str, output_version: str | None = None) -> str:
        dataset = self.datasets.get(dataset_id)
        if dataset is None:
            raise KeyError(dataset_id)
        version = output_version or dataset.get("current_output_version")
        if not version or (dataset_id, version) not in self.outputs:
            raise KeyError(version or "current output version")
        return str(version)

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        row = self.datasets.get(dataset_id)
        if row is None:
            return None
        result = copy.deepcopy(row)
        result["publish_status"] = self._publish_status(dataset_id)
        return result

    def _publish_status(self, dataset_id: str) -> str:
        rows = [row for row in self.publications if row["dataset_id"] == dataset_id]
        if not rows:
            return "unpublished"
        rows.sort(key=lambda row: (row.get("requested_at", ""), row.get("publication_id", "")), reverse=True)
        return str(rows[0]["status"])

    def _dataset_matches(self, row: dict[str, Any], **filters: Any) -> bool:
        keyword = filters.get("keyword")
        if keyword and keyword.lower() not in f"{row.get('dataset_code', '')} {row.get('dataset_title', '')}".lower():
            return False
        for name in ("data_type", "product_type", "batch_id", "grid_type", "partition_status", "quality_status"):
            if filters.get(name) is not None and row.get(name) != filters[name]:
                return False
        if filters.get("publish_status") is not None and self._publish_status(row["dataset_id"]) != filters["publish_status"]:
            return False
        for name, op in (("time_start", "start"), ("time_end", "end")):
            bound = filters.get(name)
            if bound is not None:
                try:
                    value = datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
                except ValueError:
                    value = bound
                if op == "start" and value < bound:
                    return False
                if op == "end" and value > bound:
                    return False
        return True

    def list_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: SortOrder,
    ) -> list[dict[str, Any]]:
        _validate_page(limit, offset, sort_by, sort_order, _DATASET_SORTS)
        rows = [
            self.get_dataset(key)
            for key in self.datasets
            if self._dataset_matches(
                self.datasets[key],
                keyword=keyword,
                data_type=data_type,
                product_type=product_type,
                batch_id=batch_id,
                grid_type=grid_type,
                partition_status=partition_status,
                quality_status=quality_status,
                publish_status=publish_status,
                time_start=time_start,
                time_end=time_end,
            )
        ]
        rows = [row for row in rows if row is not None]
        concrete_rows = [row for row in rows if row is not None]
        concrete_rows.sort(
            key=lambda row: (row.get(_DATASET_SORTS[sort_by]) is None, row.get(_DATASET_SORTS[sort_by]), row["dataset_id"]),
            reverse=sort_order == "desc",
        )
        return copy.deepcopy(concrete_rows[offset : offset + limit])

    def count_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
    ) -> int:
        return sum(
            self._dataset_matches(
                row,
                keyword=keyword,
                data_type=data_type,
                product_type=product_type,
                batch_id=batch_id,
                grid_type=grid_type,
                partition_status=partition_status,
                quality_status=quality_status,
                publish_status=publish_status,
                time_start=time_start,
                time_end=time_end,
            )
            for row in self.datasets.values()
        )

    def get_output_version(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        if dataset_id not in self.datasets:
            return None
        row = self.outputs.get((dataset_id, output_version))
        return copy.deepcopy(row) if row else None

    def _detail(
        self, noun: str, dataset_id: str, output_version: str | None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        _validate_page(limit, offset, sort_by, sort_order, _DETAIL_SORTS[noun])
        version = self.resolve_output_version(dataset_id, output_version)
        source = self.assets if noun == "assets" else self.bands if noun == "bands" else getattr(self, noun)
        rows = [
            row
            for row in source.values()
            if row.get("dataset_id") == dataset_id and (noun in {"assets", "bands"} or row.get("output_version") == version)
        ]
        key = _DETAIL_SORTS[noun][sort_by]
        rows.sort(
            key=lambda row: (
                row.get(key) is None,
                row.get(key),
                row.get("output_id") or row.get("source_asset_id") or row.get("band_code") or "",
            ),
            reverse=sort_order == "desc",
        )
        return copy.deepcopy(rows[offset : offset + limit])

    def count_assets(self, dataset_id: str, output_version: str | None = None) -> int:
        return len(self._detail_all("assets", dataset_id, output_version))

    def count_bands(self, dataset_id: str, output_version: str | None = None) -> int:
        return len(self._detail_all("bands", dataset_id, output_version))

    def count_tiles(self, dataset_id: str, output_version: str | None = None) -> int:
        return len(self._detail_all("tiles", dataset_id, output_version))

    def count_indexes(self, dataset_id: str, output_version: str | None = None) -> int:
        return len(self._detail_all("indexes", dataset_id, output_version))

    def count_grid_cells(self, dataset_id: str, output_version: str | None = None) -> int:
        return len(self._detail_all("grid_cells", dataset_id, output_version))

    def _detail_all(self, noun: str, dataset_id: str, output_version: str | None) -> list[dict[str, Any]]:
        version = self.resolve_output_version(dataset_id, output_version)
        source = self.assets if noun == "assets" else self.bands if noun == "bands" else getattr(self, noun)
        return [
            row
            for row in source.values()
            if row.get("dataset_id") == dataset_id and (noun in {"assets", "bands"} or row.get("output_version") == version)
        ]

    def list_assets(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._detail("assets", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def list_bands(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._detail("bands", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def list_tiles(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._detail("tiles", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def list_indexes(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._detail("indexes", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def list_grid_cells(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._detail("grid_cells", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def list_publications(self, dataset_id: str, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]:
        _validate_page(limit, offset, sort_by, sort_order, _DETAIL_SORTS["publications"])
        rows = [row for row in self.publications if row["dataset_id"] == dataset_id]
        rows.sort(key=lambda row: (row.get(sort_by) is None, row.get(sort_by), row.get("publication_id", "")), reverse=sort_order == "desc")
        return copy.deepcopy(rows[offset : offset + limit])

    def count_publications(self, dataset_id: str) -> int:
        return sum(row["dataset_id"] == dataset_id for row in self.publications)

    def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool:
        return any(row["dataset_id"] == dataset_id and row["output_version"] == output_version for row in self.publications)

    def get_output_cleanup_state(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        output = self.outputs.get((dataset_id, output_version))
        if output is None:
            return None
        return {
            "dataset_id": dataset_id,
            "output_version": output_version,
            "status": output["status"],
            "object_prefix": output["object_prefix"],
            "completed_at": output.get("completed_at"),
            "failed_at": output.get("failed_at"),
            "is_current": self.datasets.get(dataset_id, {}).get("current_output_version") == output_version,
        }

    def claim_outbox(self, worker_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = [row for row in self.outbox.values() if row["status"] == "pending"][:limit]
        for row in rows:
            row.update({"status": "processing", "attempt_count": row["attempt_count"] + 1, "claimed_at": _now(), "claimed_by": worker_id})
        return copy.deepcopy(rows)

    def acknowledge_outbox(self, event_id: str) -> None:
        row = self.outbox.get(event_id)
        if row is None:
            raise KeyError(event_id)
        row.update({"status": "delivered", "delivered_at": _now(), "claimed_at": None, "claimed_by": None})

    def retry_outbox(self, event_id: str, error: str, *, available_at: str) -> None:
        row = self.outbox.get(event_id)
        if row is None:
            raise KeyError(event_id)
        row.update({"status": "pending", "available_at": available_at, "last_error": error, "claimed_at": None, "claimed_by": None})

    # Small deterministic fixtures used by M2/M3 tests and operator diagnostics.
    def seed_dataset(self, dataset_id: str, **overrides: Any) -> dict[str, Any]:
        row = {
            "dataset_id": dataset_id,
            "dataset_code": dataset_id,
            "dataset_title": dataset_id,
            "data_type": "optical",
            "batch_id": "batch",
            "grid_type": "geohash",
            "partition_status": "completed",
            "quality_status": "pending",
            "created_at": _now(),
            "updated_at": _now(),
            "current_output_version": None,
            "quality_sequence": 0,
            "quality_error_count": 0,
            "quality_warning_count": 0,
            "partition_completed_at": _now(),
            **overrides,
        }
        self.datasets[dataset_id] = row
        return copy.deepcopy(row)

    def seed_quality_state(self, dataset_id: str, *, current_run: str | None, sequence: int, errors: int, warnings: int) -> None:
        self.datasets[dataset_id].update(
            {
                "current_quality_run_id": current_run,
                "quality_sequence": sequence,
                "quality_error_count": errors,
                "quality_warning_count": warnings,
            }
        )

    def seed_publication(
        self, dataset_id: str, *, status: str, requested_at: str, output_version: str | None = None, publication_id: str | None = None
    ) -> dict[str, Any]:
        row = {
            "publication_id": publication_id or f"pub-{len(self.publications) + 1}",
            "dataset_id": dataset_id,
            "output_version": output_version or self.datasets.get(dataset_id, {}).get("current_output_version"),
            "status": status,
            "requested_at": requested_at,
            "activated_at": requested_at if status == "active" else None,
        }
        self.publications.append(row)
        return copy.deepcopy(row)

    def outbox_rows(self) -> list[dict[str, Any]]:
        return copy.deepcopy(list(self.outbox.values()))


class OpenGaussPartitionDomainStore(InMemoryPartitionDomainStore):
    """OpenGauss-backed boundary with the same protocol as the memory store.

    SQL mutation/read implementations are layered in by the workflow adapter;
    this class intentionally keeps a real connection transaction available for
    M3 quality/publication code and fail-closes on schema version mismatches.
    """

    def __init__(self, dsn: str | None = None, *, connection_factory: Any | None = None) -> None:
        super().__init__()
        self.dsn = dsn
        self.connection_factory = connection_factory
        self.recorded_sql: list[str] = []

    def _connect(self) -> Any:
        if self.connection_factory is not None:
            return self.connection_factory()
        if not self.dsn:
            raise RuntimeError("OpenGauss DSN is required")
        from cube_web.services.db_pool import _PostgresPool

        return _PostgresPool.for_dsn(self.dsn).connection()

    def _execute(self, connection: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
        self.recorded_sql.append(" ".join(sql.split()))
        if hasattr(connection, "execute"):
            return connection.execute(sql, params)
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor

    def _fetchall(self, connection: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor = self._execute(connection, sql, params)
        rows = cursor.fetchall() if hasattr(cursor, "fetchall") else []
        description = getattr(cursor, "description", None) or []
        names = [str(getattr(item, "name", item[0] if isinstance(item, (tuple, list)) else item)) for item in description]
        return [dict(zip(names, row)) for row in rows] if names else []

    def _merge_insert(
        self,
        connection: Any,
        *,
        table: str,
        columns: tuple[str, ...],
        values: tuple[Any, ...],
        key_columns: tuple[str, ...],
    ) -> None:
        """OpenGauss-compatible idempotent insert for immutable M2 rows."""
        jsonb_columns = {"attributes", "bbox", "counts", "geometry", "payload"}
        timestamp_columns = {
            "acquisition_time",
            "time_start",
            "time_end",
        }
        uuid_columns = {
            "event_id",
            "quality_run_id",
            "publication_id",
        }
        source = ", ".join(
            f"%s::jsonb AS {column}"
            if column in jsonb_columns
            else f"%s::timestamptz AS {column}"
            if column in timestamp_columns
            else f"%s::uuid AS {column}"
            if column in uuid_columns
            else f"%s AS {column}"
            for column in columns
        )
        predicate = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
        names = ", ".join(columns)
        source_names = ", ".join(f"source.{column}" for column in columns)
        self._execute(
            connection,
            f"MERGE INTO {table} target USING (SELECT {source}) source ON ({predicate}) "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({source_names})",
            values,
        )

    def _assert_live_schema(self, connection: Any) -> None:
        rows = self._fetchall(
            connection,
            "SELECT schema_version FROM partition_domain_schema_version WHERE singleton = TRUE",
        )
        if not rows:
            raise RuntimeError("partition domain schema version row is missing")
        actual = rows[0].get("schema_version")
        if actual != SCHEMA_VERSION:
            raise RuntimeError(f"partition domain schema version {actual!r} does not match {SCHEMA_VERSION!r}")

    def transaction(self) -> AbstractContextManager[Any]:
        return self._connect()

    def ensure_schema(self) -> None:
        if self.connection_factory is None and self.dsn:
            from cube_web.services.partition_domain_schema import apply_schema, assert_schema_version

            with self._connect() as connection:
                apply_schema(connection)
                assert_schema_version(connection)
        super().ensure_schema()

    def _read_rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            return self._fetchall(connection, sql, params)

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        rows = self._read_rows(
            """SELECT d.*, COALESCE(p.status, 'unpublished') AS publish_status
               FROM partition_datasets d
               LEFT JOIN LATERAL (
                 SELECT status FROM partition_publications p
                 WHERE p.dataset_id = d.dataset_id
                 ORDER BY p.requested_at DESC, p.publication_id DESC LIMIT 1
               ) p ON TRUE WHERE d.dataset_id = %s""",
            (dataset_id,),
        )
        return rows[0] if rows else None

    def _recover_ambiguous_commit(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        """Accept a completion only when a fresh connection proves all commit facts."""
        output = self.get_output_version(dataset_id, output_version)
        dataset = self.get_dataset(dataset_id)
        events = self._read_rows(
            "SELECT event_id FROM partition_domain_outbox WHERE dataset_id = %s AND output_version = %s AND event_type = 'output-version.completed'",
            (dataset_id, output_version),
        )
        if (
            output
            and output.get("status") == "completed"
            and dataset
            and dataset.get("current_output_version") == output_version
            and len(events) == 1
        ):
            return output
        return None

    def get_output_version(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        rows = self._read_rows(
            "SELECT * FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s",
            (dataset_id, output_version),
        )
        return rows[0] if rows else None

    def resolve_output_version(self, dataset_id: str, output_version: str | None = None) -> str:
        if output_version is not None:
            if self.get_output_version(dataset_id, output_version) is None:
                raise KeyError(output_version)
            return output_version
        rows = self._read_rows(
            "SELECT current_output_version FROM partition_datasets WHERE dataset_id = %s",
            (dataset_id,),
        )
        if not rows or rows[0].get("current_output_version") is None:
            raise KeyError(dataset_id)
        return str(rows[0]["current_output_version"])

    def _dataset_query(
        self,
        *,
        filters: dict[str, Any],
        limit: int | None = None,
        offset: int = 0,
        sort_by: str = "updated_at",
        sort_order: SortOrder = "asc",
    ) -> tuple[str, tuple[Any, ...]]:
        conditions = ["1=1"]
        params: list[Any] = []
        if filters.get("keyword"):
            conditions.append("(d.dataset_code ILIKE %s OR d.dataset_title ILIKE %s)")
            keyword = f"%{filters['keyword']}%"
            params.extend((keyword, keyword))
        for name in ("data_type", "product_type", "batch_id", "grid_type", "partition_status", "quality_status"):
            if filters.get(name) is not None:
                conditions.append(f"d.{name} = %s")
                params.append(filters[name])
        if filters.get("publish_status") is not None:
            conditions.append("COALESCE(p.status, 'unpublished') = %s")
            params.append(filters["publish_status"])
        if filters.get("time_start") is not None:
            conditions.append("d.updated_at >= %s")
            params.append(filters["time_start"])
        if filters.get("time_end") is not None:
            conditions.append("d.updated_at <= %s")
            params.append(filters["time_end"])
        column = _DATASET_SORTS[sort_by]
        direction = "DESC" if sort_order == "desc" else "ASC"
        sql = f"""SELECT d.*, COALESCE(p.status, 'unpublished') AS publish_status
                  FROM partition_datasets d
                  LEFT JOIN LATERAL (
                    SELECT status FROM partition_publications p
                    WHERE p.dataset_id = d.dataset_id
                    ORDER BY p.requested_at DESC, p.publication_id DESC LIMIT 1
                  ) p ON TRUE WHERE {" AND ".join(conditions)}
                  ORDER BY d.{column} {direction} NULLS LAST, d.dataset_id {direction}"""
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend((limit, offset))
        return sql, tuple(params)

    def list_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: SortOrder,
    ) -> list[dict[str, Any]]:
        _validate_page(limit, offset, sort_by, sort_order, _DATASET_SORTS)
        filters = {
            "keyword": keyword,
            "data_type": data_type,
            "product_type": product_type,
            "batch_id": batch_id,
            "grid_type": grid_type,
            "partition_status": partition_status,
            "quality_status": quality_status,
            "publish_status": publish_status,
            "time_start": time_start,
            "time_end": time_end,
        }
        sql, params = self._dataset_query(filters=filters, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)
        rows = self._read_rows(sql, params)
        return rows

    def count_datasets(
        self,
        *,
        keyword: str | None,
        data_type: str | None,
        product_type: str | None,
        batch_id: str | None,
        grid_type: str | None,
        partition_status: str | None,
        quality_status: str | None,
        publish_status: str | None,
        time_start: datetime | None,
        time_end: datetime | None,
    ) -> int:
        filters = {
            "keyword": keyword,
            "data_type": data_type,
            "product_type": product_type,
            "batch_id": batch_id,
            "grid_type": grid_type,
            "partition_status": partition_status,
            "quality_status": quality_status,
            "publish_status": publish_status,
            "time_start": time_start,
            "time_end": time_end,
        }
        sql, params = self._dataset_query(filters=filters, limit=None)
        count_sql = f"SELECT count(*) AS count FROM ({sql}) AS filtered"
        rows = self._read_rows(count_sql, params)
        return int(rows[0]["count"]) if rows else 0

    def _detail_query(
        self,
        noun: str,
        dataset_id: str,
        output_version: str | None,
        *,
        limit: int | None,
        offset: int,
        sort_by: str = "created_at",
        sort_order: SortOrder = "asc",
    ) -> tuple[str, tuple[Any, ...]]:
        if limit is not None:
            _validate_page(limit, offset, sort_by, sort_order, _DETAIL_SORTS[noun])
        table = "partition_dataset_assets" if noun == "assets" else "partition_dataset_bands" if noun == "bands" else f"partition_{noun}"
        conditions = ["dataset_id = %s"]
        params: list[Any] = [dataset_id]
        if noun not in {"assets", "bands"}:
            conditions.append(
                "output_version = COALESCE(%s, (SELECT current_output_version FROM partition_datasets WHERE dataset_id = %s))"
            )
            params.extend((output_version, dataset_id))
        column = _DETAIL_SORTS[noun][sort_by]
        direction = "DESC" if sort_order == "desc" else "ASC"
        tie_breaker = (
            "output_id" if noun in {"tiles", "indexes", "grid_cells"} else "publication_id" if noun == "publications" else "source_asset_id"
        )
        sql = f"SELECT * FROM {table} WHERE {' AND '.join(conditions)} ORDER BY {column} {direction} NULLS LAST, {tie_breaker} {direction}"
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend((limit, offset))
        return sql, tuple(params)

    def _list_detail_db(
        self, noun: str, dataset_id: str, output_version: str | None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        sql, params = self._detail_query(
            noun, dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order
        )
        rows = self._read_rows(sql, params)
        return rows

    def _count_detail_db(self, noun: str, dataset_id: str, output_version: str | None) -> int:
        sql, params = self._detail_query(noun, dataset_id, output_version, limit=None, offset=0)
        rows = self._read_rows(f"SELECT count(*) AS count FROM ({sql}) AS filtered", params)
        return int(rows[0]["count"]) if rows else 0

    def list_assets(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._list_detail_db(
            "assets", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order
        )

    def count_assets(self, dataset_id: str, output_version: str | None = None) -> int:
        return self._count_detail_db("assets", dataset_id, output_version)

    def list_bands(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._list_detail_db("bands", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def count_bands(self, dataset_id: str, output_version: str | None = None) -> int:
        return self._count_detail_db("bands", dataset_id, output_version)

    def list_tiles(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._list_detail_db("tiles", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order)

    def count_tiles(self, dataset_id: str, output_version: str | None = None) -> int:
        return self._count_detail_db("tiles", dataset_id, output_version)

    def list_indexes(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._list_detail_db(
            "indexes", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order
        )

    def count_indexes(self, dataset_id: str, output_version: str | None = None) -> int:
        return self._count_detail_db("indexes", dataset_id, output_version)

    def list_grid_cells(
        self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        return self._list_detail_db(
            "grid_cells", dataset_id, output_version, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order
        )

    def count_grid_cells(self, dataset_id: str, output_version: str | None = None) -> int:
        return self._count_detail_db("grid_cells", dataset_id, output_version)

    def list_publications(self, dataset_id: str, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]:
        sql, params = self._detail_query(
            "publications", dataset_id, None, limit=limit, offset=offset, sort_by=sort_by, sort_order=sort_order
        )
        rows = self._read_rows(sql, params)
        return rows

    def count_publications(self, dataset_id: str) -> int:
        sql, params = self._detail_query("publications", dataset_id, None, limit=None, offset=0)
        rows = self._read_rows(f"SELECT count(*) AS count FROM ({sql}) AS filtered", params)
        return int(rows[0]["count"]) if rows else 0

    def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool:
        rows = self._read_rows(
            "SELECT EXISTS (SELECT 1 FROM partition_publications WHERE dataset_id = %s AND output_version = %s) AS referenced",
            (dataset_id, output_version),
        )
        return bool(rows[0]["referenced"]) if rows else False

    def get_output_cleanup_state(self, dataset_id: str, output_version: str) -> dict[str, Any] | None:
        rows = self._read_rows(
            "SELECT o.dataset_id,o.output_version,o.status,o.object_prefix,o.completed_at,o.failed_at,(d.current_output_version = o.output_version) AS is_current FROM partition_output_versions o JOIN partition_datasets d ON d.dataset_id = o.dataset_id WHERE o.dataset_id = %s AND o.output_version = %s",
            (dataset_id, output_version),
        )
        if not rows:
            return None
        manifest_rows = self._read_rows(
            "SELECT tile_uri,checksum,byte_size FROM partition_tiles WHERE dataset_id = %s AND output_version = %s ORDER BY output_id ASC",
            (dataset_id, output_version),
        )
        manifest = []
        for tile in manifest_rows:
            tile_uri = tile.get("tile_uri")
            object_key = str(tile_uri or "")
            if object_key.startswith("s3://"):
                object_key = object_key[5:].split("/", 1)[1] if "/" in object_key[5:] else object_key[5:]
            manifest.append(
                {"object_key": object_key, "tile_uri": tile_uri, "checksum": tile.get("checksum"), "byte_size": tile.get("byte_size")}
            )
        state = rows[0]
        state["manifest"] = manifest
        return state

    def start_output(self, request: "StrictPartitionRequest", dataset: "DatasetInput", task_id: str) -> str:
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            dataset_id = str(_field(dataset, "dataset_id"))
            version = _output_version(dataset_id, task_id)
            self._merge_insert(
                connection,
                table="partition_datasets",
                columns=(
                    "dataset_id",
                    "batch_id",
                    "dataset_code",
                    "dataset_title",
                    "data_type",
                    "product_type",
                    "attributes",
                    "grid_type",
                    "requested_grid_level",
                    "requested_grid_level_name",
                    "partition_method",
                    "cover_mode",
                    "partition_status",
                ),
                key_columns=("dataset_id",),
                values=(
                    dataset_id,
                    _field(request, "batch_id"),
                    _field(dataset, "dataset_code"),
                    _field(dataset, "dataset_title"),
                    _field(dataset, "data_type"),
                    _field(dataset, "product_type"),
                    json.dumps(_value(_field(dataset, "attributes", {}))),
                    _field(request, "grid_type"),
                    _field(request, "requested_grid_level"),
                    str(_field(request, "requested_grid_level")),
                    _field(request, "partition_method"),
                    _field(request, "cover_mode", "intersect"),
                    "running",
                ),
            )
            for asset in _field(dataset, "assets", ()):
                self._merge_insert(
                    connection,
                    table="partition_dataset_assets",
                    columns=(
                        "dataset_id", "source_asset_id", "cog_uri", "source_uri", "source_kind", "source_format", "checksum", "bbox", "crs",
                        "time_start", "time_end", "attributes",
                    ),
                    key_columns=("dataset_id", "source_asset_id"),
                    values=(
                        dataset_id,
                        _field(asset, "source_asset_id"),
                        None if _field(asset, "cog_uri") is None else str(_field(asset, "cog_uri")),
                        _asset_source_uri(asset),
                        str(_field(asset, "source_kind", "cog")),
                        str(_field(asset, "source_format", "cog")),
                        _field(asset, "checksum"),
                        json.dumps(_value(_field(asset, "bbox"))),
                        _field(asset, "crs"),
                        _field(asset, "time_start"),
                        _field(asset, "time_end"),
                        json.dumps(_value(_field(asset, "attributes", {}))),
                    ),
                )
            for band in _field(dataset, "bands", ()):
                self._merge_insert(
                    connection,
                    table="partition_dataset_bands",
                    columns=("dataset_id", "source_asset_id", "band_code", "band_name", "band_type", "unit", "display_order", "attributes"),
                    key_columns=("dataset_id", "source_asset_id", "band_code"),
                    values=(
                        dataset_id,
                        _field(band, "source_asset_id"),
                        _field(band, "band_code"),
                        _field(band, "band_name"),
                        _field(band, "band_type"),
                        _field(band, "unit"),
                        _field(band, "display_order", 0),
                        json.dumps(_value(_field(band, "attributes", {}))),
                    ),
                )
            self._merge_insert(
                connection,
                table="partition_output_versions",
                columns=(
                    "dataset_id",
                    "output_version",
                    "task_id",
                    "grid_type",
                    "requested_grid_level",
                    "requested_grid_level_name",
                    "partition_method",
                    "status",
                    "object_prefix",
                ),
                key_columns=("dataset_id", "output_version"),
                values=(
                    dataset_id,
                    version,
                    task_id,
                    _field(request, "grid_type"),
                    _field(request, "requested_grid_level"),
                    str(_field(request, "requested_grid_level")),
                    _field(request, "partition_method"),
                    "staging",
                    f"partition/{dataset_id}/versions/{version}/",
                ),
            )
            if hasattr(connection, "commit"):
                connection.commit()
        return version

    def complete_output(self, result: "PartitionDatasetResult") -> dict[str, Any]:
        dataset_id = str(_field(result, "dataset_id"))
        version = str(_field(result, "output_version"))
        task_id = str(_field(result, "task_id"))
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            self._execute(connection, "BEGIN")
            datasets = self._fetchall(connection, "SELECT * FROM partition_datasets WHERE dataset_id = %s FOR UPDATE", (dataset_id,))
            if not datasets:
                raise ValueError("dataset output has not been started")
            attempts = self._fetchall(
                connection,
                "SELECT * FROM partition_job_attempts WHERE task_id = %s FOR UPDATE",
                (task_id,),
            )
            attempt = attempts[0] if attempts else None
            attempt_payload = _value(attempt.get("payload")) if attempt else {}
            strict_dataset_ids = {
                str(item.get("dataset_id"))
                for item in attempt_payload.get("datasets", ())
                if isinstance(item, dict) and item.get("dataset_id")
            } if isinstance(attempt_payload, dict) and attempt_payload.get("strict_partition_request") else set()
            legacy_batch_matches = bool(
                attempt
                and not strict_dataset_ids
                and str(attempt.get("batch_id") or "") == str(datasets[0].get("batch_id") or "")
            )
            if not attempt or (dataset_id not in strict_dataset_ids and not legacy_batch_matches):
                raise RuntimeError("partition attempt is not active for this dataset")
            outputs = self._fetchall(
                connection,
                "SELECT * FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s FOR UPDATE",
                (dataset_id, version),
            )
            if not outputs:
                raise ValueError("output version has not been started")
            output = outputs[0]
            if output.get("task_id") != task_id:
                raise ValueError("output version task mismatch")
            if output.get("status") == "completed":
                if hasattr(connection, "commit"):
                    connection.commit()
                return output
            if output.get("status") != "staging":
                raise RuntimeError(f"output version is not staging: {output.get('status')}")
            # Entity indexes may reference their tile, so honor the FK insertion order.
            for noun in ("grid_cells", "tiles", "indexes"):
                for row in _field(result, noun, ()):
                    payload = _value(row)
                    values: tuple[Any, ...]
                    if noun == "grid_cells":
                        columns = (
                            "output_id,dataset_id,output_version,grid_type,grid_level,grid_level_name,space_code,topology_code,"
                            "bbox,geometry,tile_count,index_count"
                        )
                        values = (
                            payload.get("output_id"),
                            dataset_id,
                            version,
                            payload.get("grid_type", _field(result, "grid_type")),
                            payload.get("grid_level", _field(result, "requested_grid_level")),
                            str(payload.get("grid_level", _field(result, "requested_grid_level"))),
                            payload.get("space_code", ""),
                            payload.get("topology_code"),
                            json.dumps(payload.get("bbox")),
                            json.dumps(payload.get("geometry")),
                            payload.get("tile_count", 0),
                            payload.get("index_count", 0),
                        )
                    elif noun == "tiles":
                        columns = "output_id,dataset_id,output_version,source_asset_id,band_code,grid_type,grid_level,grid_level_name,space_code,topology_code,time_bucket,tile_uri,tile_kind,bbox,width,height,byte_size,checksum,status"
                        values = (
                            payload.get("output_id"),
                            dataset_id,
                            version,
                            payload.get("source_asset_id", ""),
                            payload.get("band_code", ""),
                            payload.get("grid_type", _field(result, "grid_type")),
                            payload.get("grid_level", _field(result, "requested_grid_level")),
                            str(payload.get("grid_level", _field(result, "requested_grid_level"))),
                            payload.get("space_code", ""),
                            payload.get("topology_code"),
                            payload.get("time_bucket", ""),
                            payload.get("tile_uri", payload.get("value_ref_uri", "s3://")),
                            payload.get("tile_kind", "logical_reference"),
                            json.dumps(payload.get("bbox")),
                            payload.get("width"),
                            payload.get("height"),
                            payload.get("byte_size"),
                            payload.get("checksum"),
                            payload.get("status", "ready"),
                        )
                    else:
                        columns = "output_id,dataset_id,output_version,tile_output_id,source_asset_id,band_code,acquisition_time,time_bucket,grid_type,grid_level,grid_level_name,topology_code,space_code,st_code,window_col_off,window_row_off,window_width,window_height,value_ref_uri,attributes"
                        values = (
                            payload.get("output_id"),
                            dataset_id,
                            version,
                            payload.get("tile_output_id"),
                            payload.get("source_asset_id", ""),
                            payload.get("band_code", ""),
                            payload.get("acquisition_time"),
                            payload.get("time_bucket", ""),
                            payload.get("grid_type", _field(result, "grid_type")),
                            payload.get("grid_level", _field(result, "requested_grid_level")),
                            str(payload.get("grid_level", _field(result, "requested_grid_level"))),
                            payload.get("topology_code"),
                            payload.get("space_code", ""),
                            payload.get("st_code", ""),
                            payload.get("window_col_off"),
                            payload.get("window_row_off"),
                            payload.get("window_width"),
                            payload.get("window_height"),
                            payload.get("value_ref_uri", payload.get("tile_uri", "s3://")),
                            json.dumps(payload.get("attributes", {})),
                        )
                    self._merge_insert(
                        connection,
                        table=f"partition_{noun}",
                        columns=tuple(columns.split(",")),
                        values=values,
                        key_columns=("output_id",),
                    )
            counts = {
                "tiles": len(_field(result, "tiles", ())),
                "indexes": len(_field(result, "indexes", ())),
                "grid_cells": len(_field(result, "grid_cells", ())),
            }
            self._execute(
                connection,
                "UPDATE partition_output_versions SET status = 'completed', completed_at = now(), tile_count = %s, index_count = %s, grid_cell_count = %s, counts = %s::jsonb WHERE dataset_id = %s AND output_version = %s",
                (counts["tiles"], counts["indexes"], counts["grid_cells"], json.dumps(counts), dataset_id, version),
            )
            self._execute(
                connection,
                "UPDATE partition_output_versions SET status = 'superseded' WHERE dataset_id = %s AND status = 'completed' AND output_version <> %s",
                (dataset_id, version),
            )
            self._execute(
                connection,
                "UPDATE partition_datasets SET current_output_version = %s, partition_status = 'completed', partition_completed_at = now(), quality_status = 'pending', current_quality_run_id = NULL, quality_error_count = 0, quality_warning_count = 0 WHERE dataset_id = %s",
                (version, dataset_id),
            )
            payload = {
                "schema_version": SCHEMA_VERSION,
                "event_type": "output-version.completed",
                "dataset_id": dataset_id,
                "output_version": version,
                "task_id": task_id,
                "grid_type": _field(result, "grid_type"),
                "requested_grid_level": _field(result, "requested_grid_level"),
                "partition_method": _field(result, "partition_method"),
                "counts": counts,
            }
            event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"cube://partition/{dataset_id}/{version}"))
            self._merge_insert(
                connection,
                table="partition_domain_outbox",
                columns=("event_id", "dataset_id", "output_version", "event_type", "payload"),
                key_columns=("dataset_id", "output_version", "event_type"),
                values=(
                    event_id,
                    dataset_id,
                    version,
                    "output-version.completed",
                    json.dumps(payload),
                ),
            )
            if hasattr(connection, "commit"):
                try:
                    connection.commit()
                except Exception:
                    recovered = self._recover_ambiguous_commit(dataset_id, version)
                    if recovered is not None:
                        return recovered
                    raise
        completed_output = self.get_output_version(dataset_id, version)
        if completed_output is None:
            raise RuntimeError("completed output version could not be read back")
        return completed_output

    def fail_output(self, dataset_id: str, output_version: str, *, error_code: str, error_message: str) -> None:
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            self._execute(
                connection,
                "UPDATE partition_output_versions SET status = 'failed', error_code = %s, error_message = %s, failed_at = now() WHERE dataset_id = %s AND output_version = %s AND status = 'staging'",
                (error_code, error_message, dataset_id, output_version),
            )
            if hasattr(connection, "commit"):
                connection.commit()
        return None

    def claim_outbox(self, worker_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            claimed_rows = self._fetchall(
                connection,
                "SELECT * FROM partition_domain_outbox WHERE status = 'pending' AND available_at <= now() ORDER BY created_at, event_id FOR UPDATE SKIP LOCKED LIMIT %s",
                (limit,),
            )
            for event in claimed_rows:
                self._execute(
                    connection,
                    "UPDATE partition_domain_outbox SET status = 'processing', attempt_count = attempt_count + 1, claimed_at = now(), claimed_by = %s WHERE event_id = %s",
                    (worker_id, event["event_id"]),
                )
            if hasattr(connection, "commit"):
                connection.commit()
        for event in claimed_rows:
            event["status"] = "processing"
            event["claimed_by"] = worker_id
            event["attempt_count"] = int(event.get("attempt_count") or 0) + 1
        return claimed_rows

    def acknowledge_outbox(self, event_id: str) -> None:
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            self._execute(
                connection,
                "UPDATE partition_domain_outbox SET status = 'delivered', delivered_at = now(), claimed_at = NULL, claimed_by = NULL WHERE event_id = %s",
                (event_id,),
            )
            if hasattr(connection, "commit"):
                connection.commit()
        return None

    def retry_outbox(self, event_id: str, error: str, *, available_at: str) -> None:
        with self.transaction() as connection:
            self._assert_live_schema(connection)
            self._execute(
                connection,
                "UPDATE partition_domain_outbox SET status = 'pending', available_at = %s, last_error = %s, claimed_at = NULL, claimed_by = NULL WHERE event_id = %s",
                (available_at, error, event_id),
            )
            if hasattr(connection, "commit"):
                connection.commit()
        return None


_partition_domain_store: PartitionDomainStore = InMemoryPartitionDomainStore()


def get_partition_domain_store() -> PartitionDomainStore:
    return _partition_domain_store


def set_partition_domain_store(store: PartitionDomainStore) -> None:
    global _partition_domain_store
    _partition_domain_store = store
