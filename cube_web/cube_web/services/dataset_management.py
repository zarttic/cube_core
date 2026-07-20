from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock
from typing import Any, Callable, Iterator, Protocol
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cube_web.services.access_control import is_admin_role, viewer_role as canonical_viewer_role
from cube_web.services.db_pool import _PostgresPool


class ManagedDatasetNotFound(LookupError):
    pass


class ManagedSceneNotFound(LookupError):
    pass


class DatasetManagementConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class ManagedDatasetQuery:
    keyword: str | None = None
    data_type: str | None = None
    product_type: str | None = None
    ingest_status: str | None = None
    quality_status: str | None = None
    publish_status: str | None = None
    archived: bool | None = None
    time_start: datetime | None = None
    time_end: datetime | None = None
    page: int = 1
    page_size: int = 20
    sort_by: str = "updated_at"
    sort_order: str = "desc"


class DatasetManagementRepository(Protocol):
    def list_datasets(self, query: ManagedDatasetQuery, *, viewer_role: str | None = None) -> dict[str, Any]: ...

    def get_dataset(self, dataset_id: str, *, viewer_role: str | None = None) -> dict[str, Any] | None: ...

    def list_hidden_roles(self, dataset_id: str) -> list[str]: ...

    def replace_hidden_roles(self, dataset_id: str, roles: tuple[str, ...], *, actor: str) -> list[str]: ...

    def action_dataset_id(self, dataset_id: str) -> str | None: ...

    def list_detail(self, dataset_id: str, detail: str, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]: ...

    def update_metadata(self, dataset_id: str, changes: dict[str, Any], *, actor: str) -> dict[str, Any]: ...

    def reassign_scene(
        self, dataset_id: str, scene_id: str, target_dataset_id: str, *, reason: str, actor: str
    ) -> dict[str, Any]: ...

    def retry_failed_band_ingest(self, dataset_id: str, band_unit_id: str, *, actor: str) -> dict[str, Any]: ...

    def archive_dataset(self, dataset_id: str, *, reason: str, actor: str) -> dict[str, Any]: ...


DETAILS = {
    "scenes",
    "assets",
    "bands",
    "outputs",
    "grid",
    "tiles",
    "indexes",
    "ingest-records",
    "quality",
    "publications",
    "provenance",
}
SORT_COLUMNS = {
    "updated_at": "updated_at",
    "created_at": "created_at",
    "dataset_code": "dataset_code",
    "scene_count": "scene_count",
}


class DatasetManagementService:
    def __init__(
        self,
        repository: DatasetManagementRepository,
        *,
        quality_hook: Callable[[str, Any], dict[str, Any]] | None = None,
        ingest_hook: Callable[[str, Any], dict[str, Any]] | None = None,
        publish_hook: Callable[[str, Any, tuple[dict[str, str], ...]], dict[str, Any]] | None = None,
        withdraw_hook: Callable[[str, str, str, Any], dict[str, Any]] | None = None,
    ) -> None:
        self.repository = repository
        self.quality_hook = quality_hook
        self.ingest_hook = ingest_hook
        self.publish_hook = publish_hook
        self.withdraw_hook = withdraw_hook

    def list_datasets(self, query: ManagedDatasetQuery, *, viewer_role: str | None = None) -> dict[str, Any]:
        if query.sort_by not in SORT_COLUMNS or query.sort_order not in {"asc", "desc"}:
            raise ValueError("unsupported dataset sort")
        return self.repository.list_datasets(query, viewer_role=viewer_role)

    def get_dataset(self, dataset_id: str, *, viewer_role: str | None = None) -> dict[str, Any]:
        dataset = self.repository.get_dataset(dataset_id, viewer_role=viewer_role)
        if dataset is None:
            raise ManagedDatasetNotFound(dataset_id)
        return dataset

    def list_hidden_roles(self, dataset_id: str) -> list[str]:
        self.get_dataset(dataset_id)
        return self.repository.list_hidden_roles(dataset_id)

    def replace_hidden_roles(self, dataset_id: str, roles: tuple[str, ...], *, actor: str) -> list[str]:
        self.get_dataset(dataset_id)
        return self.repository.replace_hidden_roles(dataset_id, roles, actor=actor)

    def list_detail(self, dataset_id: str, detail: str, *, page: int, page_size: int, viewer_role: str | None = None) -> dict[str, Any]:
        self.get_dataset(dataset_id, viewer_role=viewer_role)
        if detail not in DETAILS:
            raise ValueError(f"unknown dataset detail: {detail}")
        items, total = self.repository.list_detail(
            dataset_id, detail, limit=page_size, offset=(page - 1) * page_size
        )
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def update_metadata(self, dataset_id: str, changes: dict[str, Any], *, actor: str) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        return self.repository.update_metadata(dataset_id, changes, actor=actor)

    def reassign_scene(
        self, dataset_id: str, scene_id: str, target_dataset_id: str, *, reason: str, actor: str
    ) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        self.get_dataset(target_dataset_id)
        return self.repository.reassign_scene(
            dataset_id, scene_id, target_dataset_id, reason=reason, actor=actor
        )

    def request_quality(self, dataset_id: str, actor: Any) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        if self.quality_hook is None:
            raise DatasetManagementConflict("quality service is not configured")
        target_id = self.repository.action_dataset_id(dataset_id)
        if target_id is None:
            raise DatasetManagementConflict("managed quality is not available for this dataset")
        return self.quality_hook(target_id, actor)

    def retry_failed_band_ingest(self, dataset_id: str, band_unit_id: str, *, actor: str) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        return self.repository.retry_failed_band_ingest(dataset_id, band_unit_id, actor=actor)

    def request_ingest(self, dataset_id: str, actor: Any) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        if self.ingest_hook is None:
            raise DatasetManagementConflict("ingest service is not configured")
        target_id = self.repository.action_dataset_id(dataset_id)
        if target_id is None:
            raise DatasetManagementConflict("manual ingest requires a managed partition output")
        return self.ingest_hook(target_id, actor)

    def publish(self, dataset_id: str, actor: Any, targets: tuple[dict[str, str], ...] = ()) -> dict[str, Any]:
        dataset = self.get_dataset(dataset_id)
        if dataset.get("quality_status") != "pass":
            raise DatasetManagementConflict("publication requires a passing quality decision")
        if dataset.get("ingest_status") != "completed":
            raise DatasetManagementConflict("publication requires completed ingest")
        if self.publish_hook is None:
            raise DatasetManagementConflict("publication service is not configured")
        target_id = self.repository.action_dataset_id(dataset_id)
        if target_id is None:
            raise DatasetManagementConflict("publication requires a managed quality snapshot")
        if targets:
            return self.publish_hook(target_id, actor, targets)
        return self.publish_hook(target_id, actor)

    def withdraw(self, dataset_id: str, publication_id: str, reason: str, actor: Any) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        if self.withdraw_hook is None:
            raise DatasetManagementConflict("publication service is not configured")
        target_id = self.repository.action_dataset_id(dataset_id)
        if target_id is None:
            raise DatasetManagementConflict("publication requires a managed quality snapshot")
        return self.withdraw_hook(target_id, publication_id, reason, actor)

    def archive(self, dataset_id: str, *, reason: str, actor: str) -> dict[str, Any]:
        self.get_dataset(dataset_id)
        return self.repository.archive_dataset(dataset_id, reason=reason, actor=actor)


class InMemoryDatasetManagementRepository:
    """Small deterministic repository used by API and service tests."""

    def __init__(
        self,
        *,
        datasets: dict[str, dict[str, Any]] | None = None,
        details: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    ) -> None:
        self.datasets = copy.deepcopy(datasets or {})
        self.details = copy.deepcopy(details or {})
        self.metadata_audit: list[dict[str, Any]] = []
        self.scene_audit: list[dict[str, Any]] = []
        self.hidden_roles: dict[str, set[str]] = {}
        self._lock = RLock()

    def list_datasets(self, query: ManagedDatasetQuery, *, viewer_role: str | None = None) -> dict[str, Any]:
        with self._lock:
            rows = [self._overview(dataset_id) for dataset_id in self.datasets if self._visible(dataset_id, viewer_role)]
            rows = [row for row in rows if self._matches(row, query)]
            reverse = query.sort_order == "desc"
            rows.sort(key=lambda row: (row.get(query.sort_by) is not None, row.get(query.sort_by), row["dataset_id"]), reverse=reverse)
            total = len(rows)
            start = (query.page - 1) * query.page_size
            page = rows[start : start + query.page_size]
            all_rows = [self._overview(dataset_id) for dataset_id in self.datasets if self._visible(dataset_id, viewer_role)]
            return {
                "items": page,
                "total": total,
                "page": query.page,
                "page_size": query.page_size,
                "summary": {
                    "dataset_count": len(all_rows),
                    "scene_count": sum(row["scene_count"] for row in all_rows),
                    "ready_scene_count": sum(row["ready_scene_count"] for row in all_rows),
                    "failed_scene_count": sum(row["failed_scene_count"] for row in all_rows),
                },
            }

    def get_dataset(self, dataset_id: str, *, viewer_role: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            return self._overview(dataset_id) if dataset_id in self.datasets and self._visible(dataset_id, viewer_role) else None

    def list_hidden_roles(self, dataset_id: str) -> list[str]:
        with self._lock:
            self._dataset(dataset_id)
            return sorted(self.hidden_roles.get(dataset_id, set()))

    def replace_hidden_roles(self, dataset_id: str, roles: tuple[str, ...], *, actor: str) -> list[str]:
        with self._lock:
            self._dataset(dataset_id)
            self.hidden_roles[dataset_id] = set(roles)
            return sorted(self.hidden_roles[dataset_id])

    def action_dataset_id(self, dataset_id: str) -> str | None:
        with self._lock:
            row = self._dataset(dataset_id)
            return dataset_id if row.get("current_output_version") else None

    def list_detail(self, dataset_id: str, detail: str, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            rows = copy.deepcopy(self.details.get(dataset_id, {}).get(detail, []))
            if detail == "scenes":
                bands_by_scene: dict[str, list[dict[str, Any]]] = {}
                for band in self.details.get(dataset_id, {}).get("bands", []):
                    bands_by_scene.setdefault(str(band.get("scene_id") or ""), []).append(copy.deepcopy(band))
                for scene in rows:
                    scene["bands"] = bands_by_scene.get(str(scene.get("scene_id") or ""), [])
            if detail == "provenance":
                rows.extend(copy.deepcopy([row for row in self.scene_audit if row.get("dataset_id") == dataset_id or row.get("previous_dataset_id") == dataset_id]))
                rows.extend(copy.deepcopy([row for row in self.metadata_audit if row.get("dataset_id") == dataset_id]))
            return rows[offset : offset + limit], len(rows)

    def update_metadata(self, dataset_id: str, changes: dict[str, Any], *, actor: str) -> dict[str, Any]:
        with self._lock:
            row = self._dataset(dataset_id)
            before = {key: copy.deepcopy(row.get(key)) for key in changes}
            row.update(copy.deepcopy(changes))
            row["updated_at"] = _now_text()
            self.metadata_audit.append({
                "action": "metadata_update", "dataset_id": dataset_id, "before": before,
                "after": copy.deepcopy(changes), "changed_by": actor, "changed_at": row["updated_at"],
            })
            return self._overview(dataset_id)

    def reassign_scene(
        self, dataset_id: str, scene_id: str, target_dataset_id: str, *, reason: str, actor: str
    ) -> dict[str, Any]:
        with self._lock:
            scenes = self.details.get(dataset_id, {}).get("scenes", [])
            scene = next((row for row in scenes if row.get("scene_id") == scene_id), None)
            if scene is None:
                raise ManagedSceneNotFound(scene_id)
            if dataset_id == target_dataset_id:
                raise DatasetManagementConflict("scene already belongs to the target dataset")
            target_scenes = self.details.setdefault(target_dataset_id, {}).setdefault("scenes", [])
            if any(row.get("scene_key") == scene.get("scene_key") for row in target_scenes):
                raise DatasetManagementConflict("target dataset already contains this scene key")
            scenes.remove(scene)
            scene["dataset_id"] = target_dataset_id
            scene["updated_at"] = _now_text()
            target_scenes.append(scene)
            audit = {
                "audit_id": f"audit-{uuid4()}", "scene_id": scene_id,
                "previous_dataset_id": dataset_id, "dataset_id": target_dataset_id,
                "action": "reassign", "reason": reason, "changed_by": actor,
                "changed_at": scene["updated_at"], "relation_type": "scene_reassignment",
            }
            self.scene_audit.append(audit)
            return copy.deepcopy(audit)

    def retry_failed_band_ingest(self, dataset_id: str, band_unit_id: str, *, actor: str) -> dict[str, Any]:
        with self._lock:
            rows = self.details.get(dataset_id, {}).get("ingest-records", [])
            row = next((item for item in reversed(rows) if band_unit_id in item.get("band_unit_ids", [])), None)
            if row is None:
                raise ManagedSceneNotFound(band_unit_id)
            if row.get("status") != "failed":
                raise DatasetManagementConflict("only a failed band ingest can be retried")
            row.update(status="queued", error_message=None, updated_at=_now_text(), requested_by=actor)
            return copy.deepcopy(row)

    def archive_dataset(self, dataset_id: str, *, reason: str, actor: str) -> dict[str, Any]:
        with self._lock:
            row = self._dataset(dataset_id)
            if row.get("status") != "archived":
                row["status"] = "archived"
                row["updated_at"] = _now_text()
                self.metadata_audit.append({
                    "action": "archive", "dataset_id": dataset_id, "reason": reason,
                    "changed_by": actor, "changed_at": row["updated_at"], "physical_data_deleted": False,
                })
            return self._overview(dataset_id)

    def _dataset(self, dataset_id: str) -> dict[str, Any]:
        try:
            return self.datasets[dataset_id]
        except KeyError as exc:
            raise ManagedDatasetNotFound(dataset_id) from exc

    def _visible(self, dataset_id: str, viewer_role: str | None) -> bool:
        return viewer_role is None or is_admin_role(viewer_role) or canonical_viewer_role(viewer_role) not in self.hidden_roles.get(dataset_id, set())

    def _overview(self, dataset_id: str) -> dict[str, Any]:
        row = copy.deepcopy(self._dataset(dataset_id))
        scenes = self.details.get(dataset_id, {}).get("scenes", [])
        ingest = self.details.get(dataset_id, {}).get("ingest-records", [])
        quality = self.details.get(dataset_id, {}).get("quality", [])
        publications = self.details.get(dataset_id, {}).get("publications", [])
        times = sorted(value for value in (scene.get("acquisition_time") for scene in scenes) if value)
        bboxes = [scene["bbox"] for scene in scenes if isinstance(scene.get("bbox"), list) and len(scene["bbox"]) == 4]
        spatial_extent = None
        if bboxes:
            spatial_extent = [
                min(bbox[0] for bbox in bboxes), min(bbox[1] for bbox in bboxes),
                max(bbox[2] for bbox in bboxes), max(bbox[3] for bbox in bboxes),
            ]
        row.update(
            scene_count=len(scenes),
            ready_scene_count=sum(scene.get("status") == "available" for scene in scenes),
            failed_scene_count=sum(scene.get("status") == "failed" for scene in scenes),
            time_start=times[0] if times else None,
            time_end=times[-1] if times else None,
            ingest_status=ingest[-1].get("status") if ingest else "pending",
            quality_status=quality[-1].get("status") if quality else "pending",
            publish_status=publications[-1].get("status") if publications else "unpublished",
            archived=row.get("status") == "archived",
            spatial_extent=spatial_extent or row.get("spatial_extent"),
        )
        return row

    @staticmethod
    def _matches(row: dict[str, Any], query: ManagedDatasetQuery) -> bool:
        if query.keyword:
            needle = query.keyword.casefold()
            values = (row.get("dataset_id"), row.get("dataset_code"), row.get("dataset_title"), row.get("keywords"))
            if not any(needle in str(value or "").casefold() for value in values):
                return False
        for field in ("data_type", "product_type", "ingest_status", "quality_status", "publish_status"):
            expected = getattr(query, field)
            if expected and row.get(field) != expected:
                return False
        if query.archived is not None and row.get("archived") != query.archived:
            return False
        if query.time_start and (not row.get("time_end") or str(row["time_end"]) < query.time_start.isoformat()):
            return False
        if query.time_end and (not row.get("time_start") or str(row["time_start"]) > query.time_end.isoformat()):
            return False
        return True


class OpenGaussDatasetManagementRepository:
    def __init__(self, dsn: str | None, *, connection_factory: Any | None = None) -> None:
        self.dsn = dsn
        self.connection_factory = connection_factory

    def list_datasets(self, query: ManagedDatasetQuery, *, viewer_role: str | None = None) -> dict[str, Any]:
        where, params = self._filters(query)
        visibility, visibility_params = self._visibility_filter(viewer_role)
        where = " AND ".join(part for part in (where, visibility) if part)
        params = (*params, *visibility_params)
        base = self._overview_sql(where)
        sort = SORT_COLUMNS[query.sort_by]
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(f"SELECT * FROM ({base}) managed ORDER BY {sort} {query.sort_order}, dataset_id {query.sort_order} LIMIT %s OFFSET %s", (*params, query.page_size, (query.page - 1) * query.page_size))
            items = [self._normalize(row) for row in cursor.fetchall()]
            cursor.execute(f"SELECT COUNT(*) AS total FROM ({base}) managed", params)
            total = int(cursor.fetchone()["total"])
            cursor.execute(
                "SELECT count(DISTINCT d.dataset_id) AS dataset_count, count(s.scene_id) AS scene_count, "
                "count(s.scene_id) FILTER (WHERE s.status='available') AS ready_scene_count, "
                "count(s.scene_id) FILTER (WHERE s.status='failed') AS failed_scene_count "
                "FROM datasets d LEFT JOIN scenes s ON s.dataset_id=d.dataset_id "
                f"WHERE {self._canonical_dataset_filter('d')} AND ({visibility or 'TRUE'})",
                visibility_params,
            )
            summary = self._normalize(cursor.fetchone())
        return {"items": items, "total": total, "page": query.page, "page_size": query.page_size, "summary": summary}

    def get_dataset(self, dataset_id: str, *, viewer_role: str | None = None) -> dict[str, Any] | None:
        visibility, visibility_params = self._visibility_filter(viewer_role)
        where = " AND ".join(part for part in ("d.dataset_id = %s", visibility) if part)
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(f"SELECT * FROM ({self._overview_sql(where)}) managed", (dataset_id, *visibility_params))
            row = cursor.fetchone()
            return None if row is None else self._normalize(row)

    def list_hidden_roles(self, dataset_id: str) -> list[str]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT role FROM dataset_role_restrictions WHERE dataset_id=%s ORDER BY role", (dataset_id,))
            return [str(row["role"]) for row in cursor.fetchall()]

    def replace_hidden_roles(self, dataset_id: str, roles: tuple[str, ...], *, actor: str) -> list[str]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("DELETE FROM dataset_role_restrictions WHERE dataset_id=%s", (dataset_id,))
            if roles:
                cursor.executemany(
                    "INSERT INTO dataset_role_restrictions (dataset_id,role,created_by) VALUES (%s,%s,%s)",
                    [(dataset_id, role, actor) for role in roles],
                )
        return list(roles)

    def action_dataset_id(self, dataset_id: str) -> str | None:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT EXISTS (SELECT 1 FROM partition_datasets pd WHERE pd.dataset_id=d.dataset_id) AS output_exists
                FROM datasets d WHERE d.dataset_id=%s
                """,
                (dataset_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ManagedDatasetNotFound(dataset_id)
            return dataset_id if row["output_exists"] else None

    def list_detail(self, dataset_id: str, detail: str, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            sql, params = self._detail_sql(detail, dataset_id)
            cursor.execute(f"SELECT * FROM ({sql}) detail_rows LIMIT %s OFFSET %s", (*params, limit, offset))
            items = [self._normalize(row) for row in cursor.fetchall()]
            if detail == "scenes" and items:
                scene_ids = [str(item["scene_id"]) for item in items]
                cursor.execute(
                    """SELECT sb.* FROM scene_bands sb JOIN scene_assets sa
                       ON sa.scene_id=sb.scene_id AND sa.asset_id=sb.asset_id
                       WHERE sb.scene_id=ANY(%s::text[]) AND sa.asset_role='data'
                       ORDER BY sb.scene_id,sb.display_order,sb.band_code""",
                    (scene_ids,),
                )
                bands_by_scene: dict[str, list[dict[str, Any]]] = {}
                for band in cursor.fetchall():
                    bands_by_scene.setdefault(str(band["scene_id"]), []).append(self._normalize(band))
                band_unit_ids = [str(band["band_unit_id"]) for bands in bands_by_scene.values() for band in bands if band.get("band_unit_id")]
                cursor.execute(
                    "SELECT * FROM partition_data_unit_grid_status WHERE band_unit_id=ANY(%s::text[]) "
                    "ORDER BY band_unit_id,grid_type,grid_level",
                    (band_unit_ids,),
                )
                grid_statuses: dict[str, list[dict[str, Any]]] = {}
                for status in cursor.fetchall():
                    grid_statuses.setdefault(str(status["band_unit_id"]), []).append(self._normalize(status))
                cursor.execute(
                    "SELECT scene_id,array_agg(load_batch_id ORDER BY load_batch_id) AS source_batch_ids "
                    "FROM load_batch_scenes WHERE scene_id=ANY(%s::text[]) "
                    "AND load_status IN ('succeeded','duplicate') GROUP BY scene_id",
                    (scene_ids,),
                )
                source_batches = {str(row["scene_id"]): list(row["source_batch_ids"] or []) for row in cursor.fetchall()}
                cursor.execute(
                    """SELECT t.source_asset_id,t.band_code
                         FROM partition_publication_targets t
                         JOIN partition_publications p ON p.publication_id=t.publication_id
                        WHERE t.dataset_id=%s AND p.status='active'""",
                    (dataset_id,),
                )
                published_targets = {(str(row["source_asset_id"]), str(row["band_code"])) for row in cursor.fetchall()}
                for scene in items:
                    scene["source_batch_ids"] = source_batches.get(str(scene["scene_id"]), [])
                    scene["eligible_source_batch_ids"] = scene["source_batch_ids"]
                    scene["bands"] = []
                    for band in bands_by_scene.get(str(scene["scene_id"]), []):
                        band["publication_status"] = "active" if (
                            str(band.get("asset_id") or ""), str(band.get("band_code") or "")
                        ) in published_targets else "unpublished"
                        band["grid_statuses"] = grid_statuses.get(str(band.get("band_unit_id") or ""), [])
                        scene["bands"].append(band)
            cursor.execute(f"SELECT COUNT(*) AS total FROM ({sql}) detail_rows", params)
            total = int(cursor.fetchone()["total"])
        return items, total

    def update_metadata(self, dataset_id: str, changes: dict[str, Any], *, actor: str) -> dict[str, Any]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT dataset_title,attributes FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,))
            before = cursor.fetchone()
            if before is None:
                raise ManagedDatasetNotFound(dataset_id)
            attributes = dict(before.get("attributes") or {})
            audit = list(attributes.get("management_audit") or [])
            before_snapshot = {
                "dataset_title": before["dataset_title"],
                "description": attributes.get("description"),
                "keywords": attributes.get("keywords"),
            }
            audit.append({
                "action": "metadata_update", "changed_by": actor, "changed_at": _now_text(),
                "before": before_snapshot, "after": changes,
            })
            if "description" in changes:
                attributes["description"] = changes["description"]
            if "keywords" in changes:
                attributes["keywords"] = changes["keywords"]
            attributes["management_audit"] = audit
            title = changes.get("dataset_title", before["dataset_title"])
            cursor.execute("UPDATE datasets SET dataset_title=%s,attributes=%s,updated_at=now() WHERE dataset_id=%s", (title, Jsonb(attributes), dataset_id))
        return self.get_dataset(dataset_id) or {}

    def reassign_scene(
        self, dataset_id: str, scene_id: str, target_dataset_id: str, *, reason: str, actor: str
    ) -> dict[str, Any]:
        audit_id = f"audit-{uuid4()}"
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT dataset_id FROM scenes WHERE scene_id=%s FOR UPDATE", (scene_id,))
            scene = cursor.fetchone()
            if scene is None or scene["dataset_id"] != dataset_id:
                raise ManagedSceneNotFound(scene_id)
            if dataset_id == target_dataset_id:
                raise DatasetManagementConflict("scene already belongs to the target dataset")
            cursor.execute("SELECT 1 FROM datasets WHERE dataset_id=%s", (target_dataset_id,))
            if cursor.fetchone() is None:
                raise ManagedDatasetNotFound(target_dataset_id)
            cursor.execute(
                "SELECT 1 FROM scenes target JOIN scenes source ON source.scene_id=%s "
                "WHERE target.dataset_id=%s AND target.scene_key=source.scene_key",
                (scene_id, target_dataset_id),
            )
            if cursor.fetchone() is not None:
                raise DatasetManagementConflict("target dataset already contains this scene key")
            cursor.execute("UPDATE scenes SET dataset_id=%s,updated_at=now() WHERE scene_id=%s", (target_dataset_id, scene_id))
            cursor.execute(
                "INSERT INTO scene_dataset_audit (audit_id,scene_id,previous_dataset_id,dataset_id,action,reason,changed_by,attributes) "
                "VALUES (%s,%s,%s,%s,'reassign',%s,%s,'{}'::jsonb) RETURNING *",
                (audit_id, scene_id, dataset_id, target_dataset_id, reason, actor),
            )
            result = self._normalize(cursor.fetchone())
        return result

    def retry_failed_band_ingest(self, dataset_id: str, band_unit_id: str, *, actor: str) -> dict[str, Any]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT irs.ingest_run_id,irs.status FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id "
                "WHERE ir.dataset_id=%s AND irs.band_unit_ids ? %s ORDER BY irs.updated_at DESC LIMIT 1 FOR UPDATE",
                (dataset_id, band_unit_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise ManagedSceneNotFound(band_unit_id)
            if row["status"] != "failed":
                raise DatasetManagementConflict("only a failed band ingest can be retried")
            cursor.execute(
                "UPDATE ingest_run_scenes SET status='queued',error_message=NULL,updated_at=now(),provenance=provenance || %s "
                "WHERE ingest_run_id=%s AND band_unit_ids ? %s RETURNING *",
                (Jsonb({"retried_by": actor, "retried_at": _now_text()}), row["ingest_run_id"], band_unit_id),
            )
            result = self._normalize(cursor.fetchone())
            cursor.execute("UPDATE ingest_runs SET status='queued',error_message=NULL,completed_at=NULL WHERE ingest_run_id=%s", (row["ingest_run_id"],))
        return result

    def archive_dataset(self, dataset_id: str, *, reason: str, actor: str) -> dict[str, Any]:
        with self._connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT attributes,status FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,))
            row = cursor.fetchone()
            if row is None:
                raise ManagedDatasetNotFound(dataset_id)
            if row["status"] != "archived":
                attributes = dict(row.get("attributes") or {})
                audit = list(attributes.get("management_audit") or [])
                audit.append({"action": "archive", "reason": reason, "changed_by": actor, "changed_at": _now_text(), "physical_data_deleted": False})
                attributes["management_audit"] = audit
                cursor.execute("UPDATE datasets SET status='archived',attributes=%s,updated_at=now() WHERE dataset_id=%s", (Jsonb(attributes), dataset_id))
        return self.get_dataset(dataset_id) or {}

    @staticmethod
    def _overview_sql(where: str) -> str:
        return f"""
            SELECT d.dataset_id,d.dataset_code,d.dataset_title,d.data_type,d.product_type,d.status,
                   COALESCE((SELECT pd0.current_output_version FROM partition_datasets pd0 WHERE pd0.dataset_id=d.dataset_id), d.current_output_version) AS current_output_version,
                   d.attributes->>'description' AS description,d.attributes->'keywords' AS keywords,
                   COALESCE(
                     CASE WHEN count(s.bbox) FILTER (WHERE jsonb_typeof(s.bbox)='array') > 0 THEN json_build_array(
                       min(CASE WHEN jsonb_typeof(s.bbox)='array' THEN (s.bbox->>0)::numeric END),
                       min(CASE WHEN jsonb_typeof(s.bbox)='array' THEN (s.bbox->>1)::numeric END),
                       max(CASE WHEN jsonb_typeof(s.bbox)='array' THEN (s.bbox->>2)::numeric END),
                       max(CASE WHEN jsonb_typeof(s.bbox)='array' THEN (s.bbox->>3)::numeric END)
                     )::jsonb END, d.attributes->'spatial_extent'
                   ) AS spatial_extent,d.created_at,d.updated_at,(d.status='archived') AS archived,
                   count(s.scene_id) AS scene_count,min(s.acquisition_time) AS time_start,max(s.acquisition_time) AS time_end,
                   min(s.resolution_native) AS resolution_native,min(s.resolution_unit) AS resolution_unit,
                   min(s.resolution_m) AS resolution_m,
                   count(s.scene_id) FILTER (WHERE s.status='available') AS ready_scene_count,
                   count(s.scene_id) FILTER (WHERE s.status='failed') AS failed_scene_count,
                   COALESCE((
                     SELECT json_build_object(
                       'geohash',json_build_object(
                         'partition',count(*) FILTER (WHERE g.grid_type='geohash' AND g.partition_status='completed'),
                         'quality',count(*) FILTER (WHERE g.grid_type='geohash' AND g.quality_status IN ('pass','warn')),
                         'ingest',count(*) FILTER (WHERE g.grid_type='geohash' AND g.ingest_status='completed'),
                         'total',count(DISTINCT sb.band_unit_id)),
                       'mgrs',json_build_object(
                         'partition',count(*) FILTER (WHERE g.grid_type='mgrs' AND g.partition_status='completed'),
                         'quality',count(*) FILTER (WHERE g.grid_type='mgrs' AND g.quality_status IN ('pass','warn')),
                         'ingest',count(*) FILTER (WHERE g.grid_type='mgrs' AND g.ingest_status='completed'),
                         'total',count(DISTINCT sb.band_unit_id)),
                       'isea4h',json_build_object(
                         'partition',count(*) FILTER (WHERE g.grid_type='isea4h' AND g.partition_status='completed'),
                         'quality',count(*) FILTER (WHERE g.grid_type='isea4h' AND g.quality_status IN ('pass','warn')),
                         'ingest',count(*) FILTER (WHERE g.grid_type='isea4h' AND g.ingest_status='completed'),
                         'total',count(DISTINCT sb.band_unit_id))
                     )::jsonb
                     FROM scene_bands sb JOIN scene_assets sa ON sa.scene_id=sb.scene_id AND sa.asset_id=sb.asset_id
                     JOIN scenes gs ON gs.scene_id=sb.scene_id
                     LEFT JOIN partition_data_unit_grid_status g ON g.band_unit_id=sb.band_unit_id
                     WHERE gs.dataset_id=d.dataset_id AND sa.asset_role='data'
                   ),'{{}}'::jsonb) AS grid_summary,
                   COALESCE((SELECT ir.status FROM ingest_runs ir
                              WHERE ir.dataset_id=d.dataset_id
                                AND EXISTS (SELECT 1 FROM partition_datasets pd1
                                            WHERE pd1.dataset_id=d.dataset_id AND pd1.current_output_version IS NOT NULL
                                              AND EXISTS (SELECT 1 FROM ingest_run_scenes irs
                                                          WHERE irs.ingest_run_id=ir.ingest_run_id
                                                            AND irs.output_version=pd1.current_output_version))
                              ORDER BY ir.created_at DESC LIMIT 1),'pending') AS ingest_status,
                   COALESCE((SELECT pd.quality_status FROM partition_datasets pd WHERE pd.dataset_id=d.dataset_id),'pending') AS quality_status,
                   COALESCE((SELECT pp.status FROM partition_publications pp WHERE pp.dataset_id=d.dataset_id ORDER BY pp.requested_at DESC LIMIT 1),'unpublished') AS publish_status
            FROM datasets d LEFT JOIN scenes s ON s.dataset_id=d.dataset_id
            WHERE ({where or 'TRUE'}) AND {OpenGaussDatasetManagementRepository._canonical_dataset_filter('d')}
            GROUP BY d.dataset_id
        """

    @staticmethod
    def _canonical_dataset_filter(alias: str) -> str:
        return "TRUE"

    @staticmethod
    def _visibility_filter(viewer_role: str | None) -> tuple[str, tuple[Any, ...]]:
        if viewer_role is None or is_admin_role(viewer_role):
            return "", ()
        role = canonical_viewer_role(viewer_role)
        return "NOT EXISTS (SELECT 1 FROM dataset_role_restrictions dr WHERE dr.dataset_id=d.dataset_id AND dr.role=%s)", (role,)

    @staticmethod
    def _filters(query: ManagedDatasetQuery) -> tuple[str, tuple[Any, ...]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query.keyword:
            clauses.append("(d.dataset_id ILIKE %s OR d.dataset_code ILIKE %s OR d.dataset_title ILIKE %s OR d.attributes::text ILIKE %s)")
            params.extend([f"%{query.keyword}%"] * 4)
        for column, value in (("d.data_type", query.data_type), ("d.product_type", query.product_type)):
            if value:
                clauses.append(f"{column}=%s")
                params.append(value)
        if query.archived is not None:
            clauses.append("d.status " + ("=" if query.archived else "<>") + " 'archived'")
        if query.time_start:
            clauses.append("EXISTS (SELECT 1 FROM scenes fs WHERE fs.dataset_id=d.dataset_id AND fs.acquisition_time >= %s)")
            params.append(query.time_start)
        if query.time_end:
            clauses.append("EXISTS (SELECT 1 FROM scenes fs WHERE fs.dataset_id=d.dataset_id AND fs.acquisition_time <= %s)")
            params.append(query.time_end)
        if query.ingest_status:
            clauses.append("COALESCE((SELECT ir.status FROM ingest_runs ir WHERE ir.dataset_id=d.dataset_id ORDER BY ir.created_at DESC LIMIT 1),'pending')=%s")
            params.append(query.ingest_status)
        if query.quality_status:
            clauses.append("COALESCE((SELECT pd.quality_status FROM partition_datasets pd WHERE pd.dataset_id=d.dataset_id),'pending')=%s")
            params.append(query.quality_status)
        if query.publish_status:
            clauses.append("COALESCE((SELECT pp.status FROM partition_publications pp WHERE pp.dataset_id=d.dataset_id ORDER BY pp.requested_at DESC LIMIT 1),'unpublished')=%s")
            params.append(query.publish_status)
        return " AND ".join(clauses), tuple(params)

    @staticmethod
    def _detail_sql(detail: str, dataset_id: str) -> tuple[str, tuple[Any, ...]]:
        provenance = [
            "SELECT 'load_batch' AS relation_type,lbs.load_batch_id AS relation_id,lbs.scene_id,lbs.load_status AS status,lbs.created_at,lbs.attributes FROM load_batch_scenes lbs JOIN scenes s ON s.scene_id=lbs.scene_id WHERE s.dataset_id=%s",
            "SELECT 'partition_run',prs.partition_run_id,prs.scene_id,prs.status,prs.created_at,prs.grid_config FROM partition_run_scenes prs WHERE prs.dataset_id=%s",
            "SELECT 'ingest_run',irs.ingest_run_id,irs.scene_id,irs.status,irs.created_at,irs.provenance FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id WHERE ir.dataset_id=%s",
            "SELECT 'scene_reassignment',a.audit_id,a.scene_id,a.action,a.changed_at,a.attributes FROM scene_dataset_audit a WHERE a.dataset_id=%s OR a.previous_dataset_id=%s",
            "SELECT 'dataset_audit',audit->>'changed_at',NULL,audit->>'action',(audit->>'changed_at')::timestamptz,audit FROM datasets d CROSS JOIN LATERAL jsonb_array_elements(COALESCE(d.attributes->'management_audit','[]'::jsonb)) audit WHERE d.dataset_id=%s",
        ]
        provenance_params = 6
        queries = {
            "scenes": "SELECT * FROM scenes WHERE dataset_id=%s ORDER BY acquisition_time DESC NULLS LAST,scene_id",
            "assets": "SELECT sa.* FROM scene_assets sa JOIN scenes s ON s.scene_id=sa.scene_id WHERE s.dataset_id=%s ORDER BY sa.created_at DESC,sa.asset_id",
            "bands": "SELECT sb.* FROM scene_bands sb JOIN scenes s ON s.scene_id=sb.scene_id WHERE s.dataset_id=%s ORDER BY sb.display_order,sb.band_code",
            "outputs": """
                SELECT o.output_version,'executor-' || md5(o.task_id) AS partition_run_id,NULL::text AS scene_id,o.status,
                       json_build_object('grid_type',o.grid_type,'grid_level',o.requested_grid_level,'partition_method',o.partition_method,'counts',o.counts::json)::jsonb AS details,
                       o.created_at,COALESCE(o.completed_at,o.failed_at) AS updated_at,'partition_output' AS source_kind
                  FROM partition_output_versions o JOIN datasets d ON d.dataset_id=o.dataset_id WHERE d.dataset_id=%s
                UNION ALL SELECT prs.output_version,prs.partition_run_id,prs.scene_id,prs.status,prs.grid_config,prs.created_at,prs.updated_at,'scene_partition' FROM partition_run_scenes prs WHERE prs.dataset_id=%s
                ORDER BY created_at DESC
            """,
            "grid": """
                SELECT g.output_id,g.output_version,g.grid_type,g.grid_level,g.grid_level_name,g.space_code,g.bbox,g.geometry,g.created_at,'partition_grid_cell' AS source_kind
                  FROM partition_grid_cells g JOIN datasets d ON d.dataset_id=g.dataset_id WHERE d.dataset_id=%s
                UNION ALL SELECT 'partition-config:' || prs.partition_run_id || ':' || prs.scene_id,prs.output_version,
                       prs.grid_config->>'grid_type',NULLIF(prs.grid_config->>'requested_grid_level','')::int,
                       prs.grid_config->>'requested_grid_level_name',NULL::text,NULL::jsonb,NULL::jsonb,prs.created_at,'scene_grid_config'
                  FROM partition_run_scenes prs WHERE prs.dataset_id=%s
                ORDER BY created_at DESC
            """,
            "tiles": """
                SELECT t.output_id,t.output_version,t.space_code,t.tile_uri,t.status,t.created_at,
                       st.st_code,'partition_tile' AS source_kind,NULL::jsonb AS provenance
                  FROM partition_tiles t
                  LEFT JOIN LATERAL (
                    SELECT i.st_code
                      FROM partition_indexes i
                     WHERE i.dataset_id=t.dataset_id AND i.output_version=t.output_version
                       AND i.source_asset_id=t.source_asset_id AND i.band_code=t.band_code
                       AND i.grid_type=t.grid_type AND i.grid_level=t.grid_level
                       AND i.space_code=t.space_code AND i.time_bucket=t.time_bucket
                     ORDER BY (i.tile_output_id=t.output_id) DESC, i.created_at DESC, i.output_id
                     LIMIT 1
                  ) st ON TRUE
                  JOIN datasets d ON d.dataset_id=t.dataset_id WHERE d.dataset_id=%s
                ORDER BY created_at DESC
            """,
            "indexes": """
                SELECT i.output_id,i.output_version,i.space_code,i.value_ref_uri,i.st_code,i.created_at,'partition_index' AS source_kind,NULL::jsonb AS provenance
                  FROM partition_indexes i JOIN datasets d ON d.dataset_id=i.dataset_id WHERE d.dataset_id=%s
                ORDER BY created_at DESC
            """,
            "ingest-records": "SELECT irs.*,ir.dataset_id,ir.status AS run_status,ir.requested_by FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id WHERE ir.dataset_id=%s ORDER BY irs.updated_at DESC",
            "quality": "SELECT q.* FROM partition_quality_runs q JOIN datasets d ON d.dataset_id=q.dataset_id WHERE d.dataset_id=%s ORDER BY q.created_at DESC",
            "publications": """
                SELECT p.*,
                       COALESCE((SELECT json_agg(json_build_object(
                         'source_asset_id', t.source_asset_id, 'band_code', t.band_code
                       ) ORDER BY t.source_asset_id, t.band_code)
                       FROM partition_publication_targets t WHERE t.publication_id=p.publication_id), '[]'::json) AS targets
                  FROM partition_publications p JOIN datasets d ON d.dataset_id=p.dataset_id
                 WHERE d.dataset_id=%s ORDER BY p.requested_at DESC
            """,
            "provenance": " UNION ALL ".join(provenance) + " ORDER BY created_at DESC",
        }
        if detail not in queries:
            raise ValueError(f"unknown dataset detail: {detail}")
        params = (dataset_id,) * (provenance_params if detail == "provenance" else (2 if detail in {"outputs", "grid"} else 1))
        return queries[detail], params

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if self.connection_factory is not None:
            connection = self.connection_factory()
            if hasattr(connection, "__enter__"):
                with connection as borrowed:
                    yield borrowed
                return
            try:
                yield connection
            finally:
                close = getattr(connection, "close", None)
                if close is not None:
                    close()
            return
        if not self.dsn:
            raise RuntimeError("OpenGauss DSN is required")
        with _PostgresPool.for_dsn(self.dsn).connection() as connection:
            yield connection

    @staticmethod
    def _normalize(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: OpenGaussDatasetManagementRepository._normalize(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return [OpenGaussDatasetManagementRepository._normalize(item) for item in value]
        return value


def _now_text() -> str:
    return datetime.now(UTC).isoformat()
