from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "run_minio_tile_io_benchmark.py"


def load_benchmark_script():
    spec = importlib.util.spec_from_file_location("minio_tile_io_benchmark", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("MinIO benchmark script could not be imported")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_inputs_enforce_supported_types_and_sorted_counts() -> None:
    module = load_benchmark_script()

    assert module.parse_counts("100, 1, 10, 10") == [1, 10, 100]
    assert module.parse_object_types("entity_tile,logical_chunk,entity_tile") == [
        "entity_tile",
        "logical_chunk",
    ]

    try:
        module.parse_object_types("logical_chunk,unknown")
    except ValueError as exc:
        assert "unknown" in str(exc)
    else:
        raise AssertionError("unsupported object type was accepted")


def test_aggregate_keeps_write_and_read_series_separate() -> None:
    module = load_benchmark_script()
    rows = [
        {
            "operation": "write",
            "object_type": "entity_tile",
            "round": 1,
            "tile_count": 10,
            "payload_bytes": 100,
            "total_bytes": 1000,
            "io_sec": 0.10,
            "write_mib_per_sec": 1.0,
            "read_mib_per_sec": None,
            "valid": True,
        },
        {
            "operation": "write",
            "object_type": "entity_tile",
            "round": 2,
            "tile_count": 10,
            "payload_bytes": 100,
            "total_bytes": 1000,
            "io_sec": 0.20,
            "write_mib_per_sec": 0.5,
            "read_mib_per_sec": None,
            "valid": True,
        },
        {
            "operation": "read",
            "object_type": "logical_chunk",
            "round": 1,
            "tile_count": 10,
            "payload_bytes": 50,
            "total_bytes": 500,
            "io_sec": 0.05,
            "write_mib_per_sec": None,
            "read_mib_per_sec": 0.9,
            "valid": True,
        },
    ]

    summary = module.aggregate(rows)

    assert [(row["operation"], row["object_type"]) for row in summary] == [
        ("read", "logical_chunk"),
        ("write", "entity_tile"),
    ]
    write_summary = summary[1]
    assert write_summary["mean_io_sec"] == 0.15
    assert write_summary["mean_write_mib_per_sec"] == 0.75
    assert write_summary["mean_read_mib_per_sec"] is None


def test_sample_csv_has_fields_for_both_operations(tmp_path: Path) -> None:
    module = load_benchmark_script()
    rows = [
        {
            "operation": "write",
            "object_type": "entity_tile",
            "round": 1,
            "tile_count": 1,
            "payload_bytes": 10,
            "io_sec": 0.1,
            "write_mib_per_sec": 0.1,
            "valid": True,
        },
        {
            "operation": "read",
            "object_type": "logical_chunk",
            "round": 1,
            "tile_count": 1,
            "payload_bytes": 20,
            "payload_bytes_min": 20,
            "payload_bytes_max": 20,
            "source_object_count": 2,
            "io_sec": 0.2,
            "read_mib_per_sec": 0.2,
            "valid": True,
        },
    ]
    fields = [
        "operation",
        "object_type",
        "round",
        "tile_count",
        "payload_bytes",
        "payload_bytes_min",
        "payload_bytes_max",
        "total_bytes",
        "concurrency",
        "write_sec",
        "read_sec",
        "io_sec",
        "write_ms_per_tile",
        "read_ms_per_tile",
        "write_mib_per_sec",
        "read_mib_per_sec",
        "verify_sec",
        "cleanup_sec",
        "verified_object_count",
        "verified_byte_count",
        "source_object_count",
        "valid",
        "error",
        "prefix",
    ]
    path = tmp_path / "samples.csv"
    module.write_csv(path, rows, fields)

    with path.open(encoding="utf-8", newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert exported[1]["source_object_count"] == "2"
    assert "read_mib_per_sec" in exported[1]
