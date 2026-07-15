from copy import deepcopy

import pytest
from pydantic import ValidationError

from cube_web.services.partition_contracts import (
    OutputIdentity,
    StrictPartitionRequest,
    derive_partition_method,
    group_datasets,
    make_output_id,
    make_output_version,
    validate_partition_method,
)


def normalized_request() -> dict:
    return {
        "batch_id": "batch-01",
        "grid_type": "geohash",
        "requested_grid_level": 7,
        "partition_method": "logical",
        "cover_mode": "minimal",
        "time_granularity": "day",
        "max_cells_per_asset": 0,
        "datasets": [{
            "dataset_id": "dataset-a",
            "dataset_code": "DS-A",
            "dataset_title": "Dataset A",
            "data_type": "optical",
            "product_type": "L2A",
            "assets": [{
                "source_asset_id": "asset-a",
                "cog_uri": "s3://cube/loader/dataset-a/asset-a.tif",
                "checksum": "a" * 64,
                "bbox": [100.0, 20.0, 101.0, 21.0],
                "crs": "EPSG:4326",
                "time_start": "2026-07-01T00:00:00Z",
                "time_end": "2026-07-01T00:05:00Z",
                "attributes": {"scene_id": "scene-a"},
            }],
            "bands": [{
                "source_asset_id": "asset-a",
                "band_code": "B04",
                "band_name": "Red",
                "band_type": "spectral",
                "unit": None,
                "display_order": 4,
                "attributes": {"wavelength_nm": 665},
            }],
            "attributes": {},
        }],
    }


def test_accepts_exact_dataset_level_normalized_bands() -> None:
    request = StrictPartitionRequest.model_validate(normalized_request())
    assert request.datasets[0].bands[0].band_code == "B04"
    assert validate_partition_method(request.grid_type, request.partition_method) == "logical"


@pytest.mark.parametrize(
    ("grid_type", "minimum", "maximum"),
    [("geohash", 1, 12), ("mgrs", 0, 5), ("isea4h", 0, 15)],
)
def test_strict_request_uses_exact_m1_level_ranges(grid_type: str, minimum: int, maximum: int) -> None:
    for level in (minimum, maximum):
        payload = normalized_request()
        payload["grid_type"] = grid_type
        payload["requested_grid_level"] = level
        payload["partition_method"] = derive_partition_method(grid_type)
        assert StrictPartitionRequest.model_validate(payload).requested_grid_level == level
    for level in (minimum - 1, maximum + 1):
        payload = normalized_request()
        payload["grid_type"] = grid_type
        payload["requested_grid_level"] = level
        payload["partition_method"] = derive_partition_method(grid_type)
        with pytest.raises(ValidationError):
            StrictPartitionRequest.model_validate(payload)


def test_carbon_uses_cog_dataset_input_not_observations() -> None:
    payload = normalized_request()
    payload["datasets"][0]["data_type"] = "carbon"
    assert StrictPartitionRequest.model_validate(payload).datasets[0].assets[0].cog_uri.scheme == "s3"
    payload["datasets"][0]["observations"] = [{"latitude": 20.0, "longitude": 100.0}]
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("request", "grid_level", 7),
        ("request", "cog_workers", 0),
        ("dataset", "selected_assets", []),
        ("asset", "source_uri", "s3://cube/raw.tif"),
        ("asset", "band", "B04"),
        ("asset", "bands", ["B04"]),
        ("asset", "polarization", "VV"),
    ],
)
def test_rejects_legacy_aliases(target: str, field: str, value: object) -> None:
    payload = normalized_request()
    node = payload if target == "request" else payload["datasets"][0]
    if target == "asset":
        node = payload["datasets"][0]["assets"][0]
    node[field] = value
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


def test_rejects_mismatched_method_and_duplicate_dataset() -> None:
    with pytest.raises(ValueError, match="must be logical"):
        validate_partition_method("mgrs", "entity")
    payload = normalized_request()
    payload["datasets"].append(deepcopy(payload["datasets"][0]))
    request = StrictPartitionRequest.model_validate(payload)
    with pytest.raises(ValueError, match="duplicate dataset_id"):
        group_datasets(request)


def test_version_and_output_ids_are_deterministic_and_level_sensitive() -> None:
    assert make_output_version("dataset-a", "task-a") == make_output_version("dataset-a", "task-a")
    base = OutputIdentity(
        dataset_id="dataset-a", output_version="v1", source_asset_id="asset-a", band_code="B04",
        grid_type="mgrs", grid_level=2, space_code="50QKK1234", topology_code="mgrs-topo-v1:utm-50n:2:50QKK1234",
        time_bucket="20260701", window_identity="0:0:512:512",
    )
    changed = base.model_copy(update={"grid_level": 1})
    assert make_output_id(base) != make_output_id(changed)
    assert derive_partition_method("isea4h") == "entity"


def test_partition_method_is_required_and_must_match_derived_value() -> None:
    missing = normalized_request()
    missing.pop("partition_method")
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(missing)
    mismatched = normalized_request()
    mismatched["partition_method"] = "entity"
    with pytest.raises(ValidationError, match="must be logical"):
        StrictPartitionRequest.model_validate(mismatched)
