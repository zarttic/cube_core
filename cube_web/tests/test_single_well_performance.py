from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "run_single_well_performance.py"
PREP_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "prepare_single_well_manifest.py"


def load_performance_script():
    spec = importlib.util.spec_from_file_location("single_well_performance", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("performance script could not be imported")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_preparation_script():
    spec = importlib.util.spec_from_file_location("single_well_preparation", PREP_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("preparation script could not be imported")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def base_case(module):
    return {
        "case_id": "optical_isea4h",
        "data_type": "optical",
        "grid_type": "isea4h",
        "grid_level": 6,
        "partition_method": "entity",
        "submission_timing": {
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:00:00.100000Z",
        },
        "final_task": {"status": "completed"},
        "attempt": {
            "status": "succeeded",
            "started_at": "2026-01-01T00:00:01Z",
            "finished_at": "2026-01-01T00:00:04Z",
            "runner_result": {
                "timings": {
                    "scope": "ray_job_driver",
                    "started_at": "2026-01-01T00:00:01Z",
                    "finished_at": "2026-01-01T00:00:04Z",
                    "elapsed_sec": 3.0,
                    "phases": {"workflow.run": {"elapsed_sec": 3.0, "count": 1}},
                },
            },
        },
    "ray_job": {
            "available": True,
            "status": "SUCCEEDED",
            "elapsed_sec": 3.0,
        "start_at": "2026-01-01T00:00:00.500000Z",
    },
    "cold_start_gate": {
        "enabled": True,
        "mode": "partition_job_quiescence_then_new_submission",
        "observations": [{"active_partition_job_count": 0}],
    },
}


def test_build_summary_deduplicates_worker_scope_elapsed() -> None:
    module = load_performance_script()
    case = base_case(module)
    case["attempt"]["runner_result"]["datasets"] = [{
        "timings": {
            "units": [{
                "workers": [{
                    "scope": "raster_worker",
                    "elapsed_sec": 2.0,
                    "phases": {
                        "grid.cover": {"elapsed_sec": 1.0, "count": 1},
                        "entity.tile_write": {"elapsed_sec": 1.0, "count": 1},
                    },
                }],
            }],
        },
    }]

    summary = module.build_summary(case, "both")

    assert summary["passed"] is True
    assert summary["worker_internal_max_sec"] == 2.0
    assert summary["worker_internal_sum_sec"] == 2.0


def test_build_summary_rejects_missing_ray_job_measurement() -> None:
    module = load_performance_script()
    case = base_case(module)
    case["ray_job"] = {"available": False, "status": ""}

    summary = module.build_summary(case, "both")

    assert summary["passed"] is False
    assert summary["ray_job_available"] is False


def test_cold_start_gate_waits_for_active_partition_jobs(monkeypatch) -> None:
    module = load_performance_script()
    snapshots = iter([
        {
            "active_partition_submission_ids": ["partition-active"],
            "active_submission_ids": ["partition-active"],
        },
        {
            "active_partition_submission_ids": [],
            "active_submission_ids": ["raysubmit-unrelated"],
        },
    ])
    monkeypatch.setattr(module, "ray_jobs_snapshot", lambda address: next(snapshots))
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)

    result = module.wait_for_cold_start("http://ray-jobs", timeout_seconds=1, poll_seconds=0)

    assert result["mode"] == "partition_job_quiescence_then_new_submission"
    assert len(result["observations"]) == 2


def test_validate_manifest_accepts_complete_multiband_scene() -> None:
    module = load_performance_script()
    checksum = "a" * 64

    def raster_asset(asset_id, bands):
        return {
            "asset_id": asset_id,
            "cog_uri": f"s3://cube/cube/source/{asset_id}.tif",
            "source_kind": "cog",
            "source_format": "cog",
            "checksum": checksum,
            "bbox": [0, 0, 1, 1],
            "crs": "EPSG:4326",
            "attributes": {"width": 128, "height": 64, "count": len(bands)},
            "bands": [
                {"band_code": code, "attributes": {"source_band_index": index}}
                for index, code in enumerate(bands, start=1)
            ],
        }

    manifest = {
        "well_id": "well-full",
        "scene_mode": "full",
        "expected_band_counts": {"optical": 4},
        "grid_levels": {"geohash": 4, "mgrs": 1, "isea4h": 6},
        "max_cells_per_asset": 0,
        "carbon_max_observations": 1,
        "datasets": [
            {
                "dataset_id": "optical",
                "data_type": "optical",
                "scenes": [{"scene_id": "optical-scene", "assets": [raster_asset("optical", ["B01", "B02", "B03", "B04"])]}],
            },
            {
                "dataset_id": "radar",
                "data_type": "radar",
                "scenes": [{"scene_id": "radar-scene", "assets": [raster_asset("radar-vv", ["VV"]), raster_asset("radar-vh", ["VH"])]}],
            },
            {
                "dataset_id": "product",
                "data_type": "product",
                "scenes": [{"scene_id": "product-scene", "assets": [raster_asset("product", ["VALUE"])]}],
            },
            {
                "dataset_id": "carbon",
                "data_type": "carbon",
                "scenes": [{
                    "scene_id": "carbon-scene",
                    "assets": [{
                        "asset_id": "carbon-asset",
                        "source_uri": "s3://cube/cube/source/carbon/source.nc4",
                        "source_kind": "raw",
                        "source_format": "netcdf",
                        "checksum": checksum,
                        "bands": [{"band_code": "XCO2", "attributes": {"source_band_index": 1}}],
                    }],
                }],
            },
        ],
    }

    validated = module.validate_manifest(manifest)

    assert validated["input_profile"]["optical"]["band_count"] == 4
    assert validated["input_profile"]["radar"]["asset_count"] == 2

    invalid_band_count = copy.deepcopy(manifest)
    invalid_band_count["expected_band_counts"]["optical"] = 3
    with pytest.raises(ValueError, match="expected_band_counts.optical=4"):
        module.validate_manifest(invalid_band_count)

    missing_vh = copy.deepcopy(manifest)
    missing_vh["datasets"][1]["scenes"][0]["assets"].pop()
    with pytest.raises(ValueError, match="VV asset and one VH asset"):
        module.validate_manifest(missing_vh)


def test_validate_manifest_accepts_requested_ten_band_optical_scene() -> None:
    module = load_performance_script()
    checksum = "a" * 64
    optical = {
        "asset_id": "optical",
        "cog_uri": "s3://cube/cube/source/optical-10.tif",
        "source_kind": "cog",
        "source_format": "cog",
        "checksum": checksum,
        "bbox": [0, 0, 1, 1],
        "crs": "EPSG:4326",
        "attributes": {"width": 128, "height": 64, "count": 10},
        "bands": [
            {"band_code": f"B{index:02d}", "attributes": {"source_band_index": index}}
            for index in range(1, 11)
        ],
    }
    manifest = {
        "well_id": "well-10-band",
        "scene_mode": "full",
        "expected_band_counts": {"optical": 10},
        "grid_levels": {"geohash": 4, "mgrs": 1, "isea4h": 6},
        "max_cells_per_asset": 0,
        "carbon_max_observations": 1,
        "datasets": [
            {"dataset_id": "optical", "data_type": "optical", "scenes": [{"scene_id": "optical-scene", "assets": [optical]}]},
            {"dataset_id": "radar", "data_type": "radar", "scenes": [{"scene_id": "radar-scene", "assets": [
                {"asset_id": "radar-vv", "cog_uri": "s3://cube/cube/source/radar-vv.tif", "source_kind": "cog", "source_format": "cog", "checksum": checksum, "bbox": [0, 0, 1, 1], "crs": "EPSG:4326", "attributes": {"width": 128, "height": 64, "count": 1}, "bands": [{"band_code": "VV", "attributes": {"source_band_index": 1}}]},
                {"asset_id": "radar-vh", "cog_uri": "s3://cube/cube/source/radar-vh.tif", "source_kind": "cog", "source_format": "cog", "checksum": checksum, "bbox": [0, 0, 1, 1], "crs": "EPSG:4326", "attributes": {"width": 128, "height": 64, "count": 1}, "bands": [{"band_code": "VH", "attributes": {"source_band_index": 1}}]},
            ]}]},
            {"dataset_id": "product", "data_type": "product", "scenes": [{"scene_id": "product-scene", "assets": [{"asset_id": "product", "cog_uri": "s3://cube/cube/source/product.tif", "source_kind": "cog", "source_format": "cog", "checksum": checksum, "bbox": [0, 0, 1, 1], "crs": "EPSG:4326", "attributes": {"width": 128, "height": 64, "count": 1}, "bands": [{"band_code": "VALUE", "attributes": {"source_band_index": 1}}]}]}]},
            {"dataset_id": "carbon", "data_type": "carbon", "scenes": [{"scene_id": "carbon-scene", "assets": [{"asset_id": "carbon", "source_uri": "s3://cube/cube/source/carbon.nc4", "source_kind": "raw", "source_format": "netcdf", "checksum": checksum, "bands": [{"band_code": "XCO2", "attributes": {"source_band_index": 1}}]}]}]},
        ],
    }

    validated = module.validate_manifest(manifest, expected_optical_bands=10)

    assert validated["input_profile"]["optical"]["band_count"] == 10


def test_build_summary_rejects_warm_start_gate() -> None:
    module = load_performance_script()
    case = base_case(module)
    case["cold_start_gate"] = {"enabled": False, "observations": []}

    summary = module.build_summary(case, "both")

    assert summary["cold_start_gate_valid"] is False
    assert summary["passed"] is False


def test_validate_manifest_rejects_crop_manifest() -> None:
    module = load_performance_script()
    with pytest.raises(ValueError, match="scene_mode=full"):
        module.validate_manifest({"well_id": "well", "scene_mode": "crop", "datasets": []})


def test_prepare_raster_preserves_full_scene_bands(tmp_path: Path) -> None:
    module = load_preparation_script()
    source_path = tmp_path / "source.tif"
    output_path = tmp_path / "prepared.tif"
    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "width": 32,
        "height": 24,
        "count": 2,
        "crs": "EPSG:4326",
        "transform": from_origin(110, 30, 0.01, 0.01),
    }
    with rasterio.open(source_path, "w", **profile) as destination:
        destination.write(np.ones((2, 24, 32), dtype=np.uint16))

    metadata = module.prepare_raster(source_path, output_path)

    assert metadata["width"] == 32
    assert metadata["height"] == 24
    assert metadata["count"] == 2
    assert metadata["source_band_indexes"] == [1, 2]
    with rasterio.open(output_path) as prepared:
        assert (prepared.width, prepared.height, prepared.count) == (32, 24, 2)
