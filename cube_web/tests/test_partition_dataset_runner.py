import pytest

from cube_web.services.partition_dataset_runner import (
    _wait_for_ray_result,
    _carbon_index_attributes,
    _consume_observation_budget,
    _record_asset_cell,
    _source_band_index,
    _normalize_wgs84_bbox,
)


class _FakeRay:
    def __init__(self) -> None:
        self.cancelled: list[tuple[object, bool]] = []

    def cancel(self, ref, *, force: bool) -> None:
        self.cancelled.append((ref, force))

    def wait(self, *_args, **_kwargs):
        return [], []

    def get(self, ref):
        return {"ref": ref}


def test_normalize_wgs84_bbox_clamps_raster_edges() -> None:
    assert _normalize_wgs84_bbox([-180.0044, -90.0022, 180.0044, 90.0022]) == [-180.0, -90.0, 180.0, 90.0]


def test_carbon_unique_cells_are_not_limited_per_asset() -> None:
    cells: set[tuple[str, int, str | None]] = set()
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    _record_asset_cell(cells, ("u4ps", 5, None), max_cells_per_asset=1)

    assert cells == {("u4pr", 5, None), ("u4ps", 5, None)}


def test_ray_wait_cancels_active_remote_task() -> None:
    ray = _FakeRay()

    with pytest.raises(Exception, match="Partition task cancelled"):
        _wait_for_ray_result(ray, "ray-ref", lambda: True)

    assert ray.cancelled == [("ray-ref", True)]


def test_carbon_observation_budget_is_shared_across_assets() -> None:
    remaining = 100
    remaining = _consume_observation_budget(remaining, 60)
    assert remaining == 40
    remaining = _consume_observation_budget(remaining, 40)
    assert remaining == 0
    assert _consume_observation_budget(None, 1000) is None


def test_carbon_index_attributes_preserve_normalized_observation_fields() -> None:
    footprint = {"type": "Point", "coordinates": [116.3, 39.9]}
    row = {
        "satellite": "OCO2",
        "observation_id": "obs-1",
        "xco2": 410.25,
        "quality_flag": "0",
        "center_lon": 116.3,
        "center_lat": 39.9,
        "footprint_geojson": footprint,
        "source_index": None,
        "metadata_json": '{"orbit": 42}',
        "product_type": "xco2",
    }

    assert _carbon_index_attributes(row, source_index=7) == {
        "satellite": "OCO2",
        "observation_id": "obs-1",
        "xco2": 410.25,
        "quality_flag": "0",
        "center_lon": 116.3,
        "center_lat": 39.9,
        "footprint_geojson": footprint,
        "source_index": 7,
        "metadata_json": '{"orbit": 42}',
        "product_type": "xco2",
    }


def test_source_band_index_prefers_loader_metadata_and_validates_bounds() -> None:
    assert _source_band_index({"display_order": 0, "attributes": {"source_band_index": 3}}, 4) == 3
    assert _source_band_index({"display_order": 1, "attributes": {}}, 4) == 2
    with pytest.raises(ValueError, match="outside raster band count"):
        _source_band_index({"display_order": 4, "attributes": {}}, 4)
