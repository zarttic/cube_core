from copy import deepcopy

import pytest
from pydantic import ValidationError

from cube_web.services.partition_contracts import (
    OutputIdentity,
    StrictPartitionRequest,
    derive_partition_method,
    effective_dataset_request,
    group_datasets,
    make_output_id,
    make_output_version,
    resolve_dataset_partition,
    validate_partition_method,
)
from cube_web.schemas import SpatiotemporalQueryRequest


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
def test_strict_request_uses_exact_production_level_ranges(grid_type: str, minimum: int, maximum: int) -> None:
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


def test_carbon_uses_explicit_raw_dataset_source_not_observations() -> None:
    payload = normalized_request()
    dataset = payload["datasets"][0]
    dataset["data_type"] = "carbon"
    dataset["product_type"] = "xco2"
    asset = dataset["assets"][0]
    asset.pop("cog_uri")
    asset.pop("bbox")
    asset.pop("crs")
    asset.update({"source_uri": "s3://cube/cube/source/carbon/oco2.nc4", "source_kind": "raw", "source_format": "netcdf"})
    request = StrictPartitionRequest.model_validate(payload)
    assert request.datasets[0].assets[0].source_uri is not None
    assert request.datasets[0].assets[0].source_format == "netcdf"
    assert request.datasets[0].assets[0].bbox is None
    assert request.datasets[0].assets[0].crs is None
    payload["datasets"][0]["observations"] = [{"latitude": 20.0, "longitude": 100.0}]
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


def test_carbon_contract_normalizes_tansat_product_type() -> None:
    payload = normalized_request()
    dataset = payload["datasets"][0]
    dataset["data_type"] = "carbon"
    dataset["product_type"] = "tansat_xco2"
    asset = dataset["assets"][0]
    asset.pop("cog_uri")
    asset.pop("bbox")
    asset.pop("crs")
    asset.update({"source_uri": "s3://cube/cube/source/carbon/tansat.h5", "source_kind": "raw", "source_format": "hdf5"})

    request = StrictPartitionRequest.model_validate(payload)

    assert request.datasets[0].product_type == "tansat"


def test_carbon_contract_rejects_unknown_product_type() -> None:
    payload = normalized_request()
    dataset = payload["datasets"][0]
    dataset["data_type"] = "carbon"
    dataset["product_type"] = "unknown"
    asset = dataset["assets"][0]
    asset.pop("cog_uri")
    asset.pop("bbox")
    asset.pop("crs")
    asset.update({"source_uri": "s3://cube/cube/source/carbon/unknown.h5", "source_kind": "raw", "source_format": "hdf5"})

    with pytest.raises(ValidationError, match="Unsupported carbon product_type"):
        StrictPartitionRequest.model_validate(payload)


def test_carbon_query_contract_normalizes_tansat_product_type() -> None:
    request = SpatiotemporalQueryRequest(
        product_type="tansat_xco2",
        time_start="2026-04-24T00:00:00Z",
        time_end="2026-04-25T00:00:00Z",
    )

    assert request.product_type == "tansat"


def test_non_carbon_cog_still_requires_bbox_and_crs() -> None:
    payload = normalized_request()
    payload["datasets"][0]["assets"][0].pop("bbox")
    with pytest.raises(ValidationError, match="require bbox and crs"):
        StrictPartitionRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("source_format", "source_uri"),
    [
        ("cog", "s3://cube/cube/source/carbon/oco2.nc4"),
        ("netcdf", "s3://cube/cube/source/carbon/oco2.hdf5"),
        ("hdf5", "s3://cube/cube/source/carbon/oco2.nc4"),
    ],
)
def test_carbon_raw_source_format_and_suffix_are_strict(source_format: str, source_uri: str) -> None:
    payload = normalized_request()
    dataset = payload["datasets"][0]
    dataset["data_type"] = "carbon"
    dataset["product_type"] = "xco2"
    asset = dataset["assets"][0]
    asset.pop("cog_uri")
    asset.update({"source_uri": source_uri, "source_format": source_format})
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("request", "grid_level", 7),
        ("request", "cog_workers", 0),
        ("dataset", "selected_assets", []),
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


def test_dataset_partition_overrides_resolve_and_validate_independently() -> None:
    payload = normalized_request()
    payload["datasets"][0]["partition"] = {
        "grid_type": "isea4h",
        "requested_grid_level": 1,
        "partition_method": "entity",
        "max_cells_per_asset": 50,
    }

    request = StrictPartitionRequest.model_validate(payload)
    effective = effective_dataset_request(request, request.datasets[0])

    assert effective.grid_type == "isea4h"
    assert effective.requested_grid_level == 1
    assert effective.partition_method == "entity"
    assert effective.max_cells_per_asset == 0

    payload["datasets"][0]["partition"]["partition_method"] = "logical"
    with pytest.raises(ValidationError, match="must be entity"):
        StrictPartitionRequest.model_validate(payload)


@pytest.mark.parametrize(
    "second_partition",
    [
        {"grid_type": "mgrs", "requested_grid_level": 1, "partition_method": "logical"},
        {"grid_type": "geohash", "requested_grid_level": 6, "partition_method": "logical"},
    ],
    ids=("different-grid-type", "different-grid-level"),
)
def test_strict_request_rejects_mixed_grid_configuration_across_datasets(second_partition) -> None:
    payload = normalized_request()
    second = deepcopy(payload["datasets"][0])
    second.update({"dataset_id": "dataset-b", "dataset_code": "DS-B", "dataset_title": "Dataset B"})
    second["partition"] = second_partition
    payload["datasets"].append(second)

    with pytest.raises(ValidationError, match="same grid type and level"):
        StrictPartitionRequest.model_validate(payload)


def test_max_observations_is_carbon_only_and_resolves_per_dataset() -> None:
    payload = normalized_request()
    dataset = payload["datasets"][0]
    dataset["data_type"] = "carbon"
    dataset["product_type"] = "xco2"
    asset = dataset["assets"][0]
    asset.pop("cog_uri")
    asset.update({"source_uri": "s3://cube/cube/source/carbon/oco2.nc4", "source_kind": "raw", "source_format": "netcdf"})
    dataset["partition"] = {"max_observations": 100}
    request = StrictPartitionRequest.model_validate(payload)
    assert resolve_dataset_partition(request, request.datasets[0]).max_observations == 100

    payload = normalized_request()
    payload["datasets"][0]["partition"] = {"max_observations": 100}
    with pytest.raises(ValidationError, match="only valid for carbon"):
        StrictPartitionRequest.model_validate(payload)
