from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cube_web.services.partition_domain_store import PartitionDomainStore
from cube_web.services.quality_contracts import DEFAULT_PAGE_SIZE, Page, SortOrder, page_offset, validate_sort


class PartitionDatasetNotFound(LookupError):
    pass


class PartitionOutputVersionNotFound(LookupError):
    pass


_DATASET_SORT = {
    "updated_at": "updated_at",
    "created_at": "created_at",
    "dataset_code": "dataset_code",
    "partition_completed_at": "partition_completed_at",
    "quality_status": "quality_status",
}
_DETAIL_SORT = {
    "assets": {"source_asset_id": "source_asset_id", "created_at": "created_at"},
    "bands": {"display_order": "display_order", "band_code": "band_code"},
    "tiles": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "indexes": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "grid": {"created_at": "created_at", "output_id": "output_id", "space_code": "space_code", "grid_level": "grid_level"},
    "publications": {"requested_at": "requested_at", "activated_at": "activated_at", "status": "status"},
}


@dataclass(frozen=True)
class DatasetQuery:
    keyword: str | None = None
    data_type: str | None = None
    product_type: str | None = None
    batch_id: str | None = None
    grid_type: str | None = None
    partition_status: str | None = None
    quality_status: str | None = None
    publish_status: str | None = None
    time_start: datetime | None = None
    time_end: datetime | None = None
    page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    sort_by: str = "updated_at"
    sort_order: SortOrder = "desc"


class DatasetService:
    def __init__(self, store: PartitionDomainStore) -> None:
        self.store = store

    def list_datasets(self, query: DatasetQuery) -> Page[dict[str, Any]]:
        sort_by, sort_order = validate_sort(query.sort_by, query.sort_order, set(_DATASET_SORT))
        offset = page_offset(query.page, query.page_size)
        filters = self._filters(query)
        items = self.store.list_datasets(
            **filters,
            limit=query.page_size,
            offset=offset,
            sort_by=_DATASET_SORT[sort_by],
            sort_order=sort_order,
        )
        total = self.store.count_datasets(**filters)
        return Page(items=tuple(items), total=total, page=query.page, page_size=query.page_size)

    def get_dataset(self, dataset_id: str) -> dict[str, Any]:
        dataset = self.store.get_dataset(dataset_id)
        if dataset is None:
            raise PartitionDatasetNotFound(dataset_id)
        return dataset

    def list_detail(
        self,
        dataset_id: str,
        detail: str,
        *,
        output_version: str | None,
        page: int,
        page_size: int,
        sort_by: str,
        sort_order: str,
    ) -> Page[dict[str, Any]]:
        self.get_dataset(dataset_id)
        if detail not in _DETAIL_SORT:
            raise ValueError(f"unknown dataset detail: {detail}")
        selected_version = self._resolve_output_version(dataset_id, output_version) if detail in {"tiles", "indexes", "grid"} else None
        selected_sort, selected_order = validate_sort(sort_by, sort_order, set(_DETAIL_SORT[detail]))
        offset = page_offset(page, page_size)
        list_method, count_method = self._detail_methods(detail)
        items = list_method(
            dataset_id,
            selected_version,
            limit=page_size,
            offset=offset,
            sort_by=_DETAIL_SORT[detail][selected_sort],
            sort_order=selected_order,
        ) if detail != "publications" else list_method(
            dataset_id,
            limit=page_size,
            offset=offset,
            sort_by=_DETAIL_SORT[detail][selected_sort],
            sort_order=selected_order,
        )
        total = count_method(dataset_id, selected_version) if detail != "publications" else count_method(dataset_id)
        return Page(items=tuple(items), total=total, page=page, page_size=page_size)

    @staticmethod
    def _filters(query: DatasetQuery) -> dict[str, Any]:
        return {
            "keyword": query.keyword,
            "data_type": query.data_type,
            "product_type": query.product_type,
            "batch_id": query.batch_id,
            "grid_type": query.grid_type,
            "partition_status": query.partition_status,
            "quality_status": query.quality_status,
            "publish_status": query.publish_status,
            "time_start": query.time_start,
            "time_end": query.time_end,
        }

    def _resolve_output_version(self, dataset_id: str, output_version: str | None) -> str:
        try:
            return self.store.resolve_output_version(dataset_id, output_version)
        except KeyError as exc:
            raise PartitionOutputVersionNotFound(str(exc)) from exc

    def _detail_methods(self, detail: str):
        methods = {
            "assets": (self.store.list_assets, self.store.count_assets),
            "bands": (self.store.list_bands, self.store.count_bands),
            "tiles": (self.store.list_tiles, self.store.count_tiles),
            "indexes": (self.store.list_indexes, self.store.count_indexes),
            "grid": (self.store.list_grid_cells, self.store.count_grid_cells),
            "publications": (self.store.list_publications, self.store.count_publications),
        }
        return methods[detail]
