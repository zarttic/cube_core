#!/usr/bin/env python3
"""Benchmark MinIO I/O using real partition tile objects.

Writes use only a real ISEA4H entity TIFF payload copied to an isolated
temporary prefix at several object counts. Reads use existing formal MinIO
logical gzip chunks and ISEA4H entity TIFF objects directly. Read counts above
the discovered pool size cycle through that real object pool.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from minio import Minio
from minio.deleteobjects import DeleteObject

REPO_ROOT = Path(__file__).resolve().parents[2]
for package_root in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from cube_split import runtime_config  # noqa: E402

DEFAULT_COUNTS = (1, 10, 100, 1000)
DEFAULT_READ_OBJECT_TYPES = ("logical_chunk", "entity_tile")
DEFAULT_OBJECT_TYPES = DEFAULT_READ_OBJECT_TYPES
WRITE_OBJECT_TYPE = "entity_tile"


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def elapsed(started: float) -> float:
    return round(time.perf_counter() - started, 6)


def parse_counts(value: str) -> list[int]:
    counts = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not counts or any(count < 1 for count in counts):
        raise ValueError("counts must contain positive integers")
    return counts


def parse_object_types(value: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(values) - set(DEFAULT_OBJECT_TYPES))
    if not values or unknown:
        raise ValueError(f"unsupported object types: {unknown or values}")
    return list(dict.fromkeys(values))


def make_client(settings: Any) -> Minio:
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def _formal_output_pairs(source_root: Path) -> dict[tuple[str, str], str]:
    pairs: dict[tuple[str, str], str] = {}
    for raw_path in sorted(source_root.glob("round_*/**/raw_cases.json")):
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
        for case in payload:
            result = ((case.get("final_task") or {}).get("result") or {})
            for dataset in result.get("datasets") or []:
                dataset_id = str(dataset.get("dataset_id") or "").strip()
                output_version = str(dataset.get("output_version") or "").strip()
                if dataset_id and output_version:
                    pairs[(dataset_id, output_version)] = str(case.get("case_id") or "unknown")
    if not pairs:
        raise RuntimeError(f"no completed partition output versions found under {source_root}")
    return pairs


def discover_samples(client: Minio, bucket: str, source_root: Path) -> dict[str, list[dict[str, Any]]]:
    candidates: dict[str, list[dict[str, Any]]] = {"entity_tile": [], "logical_chunk": []}
    for (dataset_id, output_version), case_id in _formal_output_pairs(source_root).items():
        prefix = f"partition/{dataset_id}/versions/{output_version}/"
        for item in client.list_objects(bucket, prefix=prefix, recursive=True):
            object_name = str(item.object_name)
            size_bytes = int(item.size or 0)
            if size_bytes <= 0:
                continue
            suffix = object_name.rsplit("/", 1)[-1].lower()
            if suffix.endswith(".tif"):
                object_type = "entity_tile"
            elif suffix.endswith(".gz"):
                object_type = "logical_chunk"
            else:
                continue
            candidates[object_type].append({
                "object_type": object_type,
                "case_id": case_id,
                "source_uri": f"s3://{bucket}/{object_name}",
                "bucket": bucket,
                "object_key": object_name,
                "size_bytes": size_bytes,
            })
    missing = [name for name, values in candidates.items() if not values]
    if missing:
        raise RuntimeError(f"no representative MinIO objects found for: {', '.join(missing)}")
    return {name: sorted(values, key=lambda item: (item["size_bytes"], item["source_uri"])) for name, values in candidates.items()}


def read_object(client: Minio, bucket: str, key: str) -> bytes:
    response = client.get_object(bucket, key)
    chunks: list[bytes] = []
    try:
        for chunk in response.stream(1024 * 1024):
            chunks.append(chunk)
    finally:
        response.close()
        response.release_conn()
    return b"".join(chunks)


def parallel_call(
    operation: Callable[[str], int],
    keys: list[str],
    concurrency: int,
) -> tuple[float, int, list[str]]:
    started = time.perf_counter()
    completed = 0
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(concurrency, len(keys))) as pool:
        futures = [pool.submit(operation, key) for key in keys]
        for future in as_completed(futures):
            try:
                completed += int(future.result())
            except Exception as exc:  # pragma: no cover - exercised by live MinIO failures
                errors.append(str(exc)[:500])
    return elapsed(started), completed, errors


def verify_prefix(client: Minio, bucket: str, prefix: str, expected_count: int, expected_size: int) -> dict[str, Any]:
    started = time.perf_counter()
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    actual_size = sum(int(item.size or 0) for item in objects)
    return {
        "elapsed_sec": elapsed(started),
        "object_count": len(objects),
        "byte_count": actual_size,
        "valid": len(objects) == expected_count and actual_size == expected_count * expected_size,
    }


def cleanup_prefix(client: Minio, bucket: str, keys: list[str]) -> tuple[float, list[str]]:
    started = time.perf_counter()
    errors: list[str] = []
    if keys:
        for error in client.remove_objects(bucket, (DeleteObject(key) for key in keys)):
            errors.append(str(error)[:500])
    return elapsed(started), errors


def write_one(client: Minio, bucket: str, key: str, payload: bytes, object_type: str) -> int:
    client.put_object(
        bucket,
        key,
        BytesIO(payload),
        len(payload),
        content_type="image/tiff" if object_type == "entity_tile" else "application/gzip",
        metadata={"benchmark-object-type": object_type},
    )
    return 1


def run_write_level(
    client: Minio,
    bucket: str,
    prefix: str,
    payload: bytes,
    round_number: int,
    count: int,
    concurrency: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    object_type = WRITE_OBJECT_TYPE
    keys = [f"{prefix}/write/{object_type}/round_{round_number}/count_{count}/tile_{index:08d}.tif" for index in range(count)]
    write_seconds, write_completed, write_errors = parallel_call(
        lambda key: write_one(client, bucket, key, payload, object_type),
        keys,
        concurrency,
    )
    verification = verify_prefix(client, bucket, f"{prefix}/write/{object_type}/round_{round_number}/count_{count}/", count, len(payload))
    cleanup_seconds, cleanup_errors = cleanup_prefix(client, bucket, keys)
    errors = write_errors + cleanup_errors
    valid = (
        not errors
        and verification["valid"]
        and write_completed == count
    )
    row = {
        "operation": "write",
        "object_type": object_type,
        "round": round_number,
        "tile_count": count,
        "payload_bytes": len(payload),
        "total_bytes": count * len(payload),
        "concurrency": concurrency,
        "write_sec": write_seconds,
        "read_sec": None,
        "io_sec": write_seconds,
        "write_ms_per_tile": round(write_seconds * 1000 / count, 6),
        "read_ms_per_tile": None,
        "write_mib_per_sec": round((count * len(payload)) / (1024 * 1024) / write_seconds, 6) if write_seconds else None,
        "read_mib_per_sec": None,
        "verify_sec": verification["elapsed_sec"],
        "cleanup_sec": cleanup_seconds,
        "verified_object_count": verification["object_count"],
        "verified_byte_count": verification["byte_count"],
        "valid": valid,
        "error": "; ".join(errors),
        "prefix": prefix,
    }
    phases = [
        {"operation": "write", "object_type": object_type, "round": round_number, "tile_count": count, "phase": "write", "elapsed_sec": write_seconds},
        {"operation": "write", "object_type": object_type, "round": round_number, "tile_count": count, "phase": "verify", "elapsed_sec": verification["elapsed_sec"]},
        {"operation": "write", "object_type": object_type, "round": round_number, "tile_count": count, "phase": "cleanup", "elapsed_sec": cleanup_seconds},
    ]
    return row, phases


def run_read_level(
    client: Minio,
    payload_candidates: list[dict[str, Any]],
    object_type: str,
    round_number: int,
    count: int,
    concurrency: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    selected = [payload_candidates[index % len(payload_candidates)] for index in range(count)]
    keys = [str(item["object_key"]) for item in selected]
    expected_bytes = sum(int(item["size_bytes"]) for item in selected)
    read_seconds, read_completed, read_errors = parallel_call(
        lambda key: len(read_object(client, str(payload_candidates[0]["bucket"]), key)),
        keys,
        concurrency,
    )
    valid = not read_errors and read_completed == expected_bytes
    payload_sizes = [int(item["size_bytes"]) for item in selected]
    row = {
        "operation": "read",
        "object_type": object_type,
        "round": round_number,
        "tile_count": count,
        "payload_bytes": round(statistics.mean(payload_sizes)),
        "payload_bytes_min": min(payload_sizes),
        "payload_bytes_max": max(payload_sizes),
        "total_bytes": expected_bytes,
        "concurrency": concurrency,
        "write_sec": None,
        "read_sec": read_seconds,
        "io_sec": read_seconds,
        "write_ms_per_tile": None,
        "read_ms_per_tile": round(read_seconds * 1000 / count, 6),
        "write_mib_per_sec": None,
        "read_mib_per_sec": round(expected_bytes / (1024 * 1024) / read_seconds, 6) if read_seconds else None,
        "verify_sec": None,
        "cleanup_sec": 0.0,
        "verified_object_count": None,
        "verified_byte_count": read_completed,
        "valid": valid,
        "error": "; ".join(read_errors),
        "prefix": "",
        "source_object_count": len(payload_candidates),
    }
    phases = [{
        "operation": "read",
        "object_type": object_type,
        "round": round_number,
        "tile_count": count,
        "phase": "read",
        "elapsed_sec": read_seconds,
    }]
    return row, phases


def mean(values: list[float]) -> float | None:
    return round(statistics.mean(values), 6) if values else None


def stdev(values: list[float]) -> float | None:
    return round(statistics.stdev(values), 6) if len(values) >= 2 else 0.0 if values else None


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    keys = sorted({(row["operation"], row["object_type"], row["tile_count"]) for row in rows})
    for operation, object_type, tile_count in keys:
        group = [row for row in rows if row["operation"] == operation and row["object_type"] == object_type and row["tile_count"] == tile_count]
        valid = [row for row in group if row["valid"]]
        io_seconds = [float(row["io_sec"]) for row in valid]
        write_rates = [float(row["write_mib_per_sec"]) for row in valid if row["write_mib_per_sec"] is not None]
        read_rates = [float(row["read_mib_per_sec"]) for row in valid if row["read_mib_per_sec"] is not None]
        output.append({
            "operation": operation,
            "object_type": object_type,
            "tile_count": tile_count,
            "round_count": len(group),
            "valid_round_count": len(valid),
            "payload_bytes": group[0]["payload_bytes"],
            "total_bytes": group[0]["total_bytes"],
            "mean_io_sec": mean(io_seconds),
            "stdev_io_sec": stdev(io_seconds),
            "min_io_sec": min(io_seconds) if io_seconds else None,
            "max_io_sec": max(io_seconds) if io_seconds else None,
            "mean_write_mib_per_sec": mean(write_rates),
            "mean_read_mib_per_sec": mean(read_rates),
            "pass_rate": round(len(valid) / len(group), 6) if group else None,
        })
    return output


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, help="formal multi-round output root containing raw_cases.json")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--counts", default=",".join(str(value) for value in DEFAULT_COUNTS))
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--write-object-types", default=WRITE_OBJECT_TYPE)
    parser.add_argument("--read-object-types", default=",".join(DEFAULT_READ_OBJECT_TYPES))
    parser.add_argument("--read-pool-size", type=int, default=4)
    parser.add_argument("--prefix", default=None)
    args = parser.parse_args(argv)
    if args.rounds < 1 or args.concurrency < 1:
        raise ValueError("rounds and concurrency must be positive")
    counts = parse_counts(args.counts)
    write_object_types = parse_object_types(args.write_object_types)
    read_object_types = parse_object_types(args.read_object_types)
    if write_object_types != [WRITE_OBJECT_TYPE]:
        raise ValueError("formal MinIO write benchmark only supports entity_tile from isea4h")
    if args.read_pool_size < 1:
        raise ValueError("read-pool-size must be positive")
    source_root = Path(args.source_root).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else source_root
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = runtime_config.minio_settings()
    if not all((settings.endpoint, settings.access_key, settings.secret_key, settings.bucket)):
        raise RuntimeError("MinIO runtime configuration is incomplete")
    client = make_client(settings)
    candidates = discover_samples(client, settings.bucket, source_root)
    sample_payloads: dict[str, bytes] = {}
    sample_fetch: dict[str, float] = {}
    write_sample = min(candidates[WRITE_OBJECT_TYPE], key=lambda item: (item["size_bytes"], item["source_uri"]))
    for object_type in [WRITE_OBJECT_TYPE]:
        started = time.perf_counter()
        sample = write_sample
        payload = read_object(client, sample["bucket"], sample["object_key"])
        if len(payload) != sample["size_bytes"]:
            raise RuntimeError(f"sample object size changed: {sample['source_uri']}")
        sample_payloads[object_type] = payload
        sample_fetch[object_type] = elapsed(started)

    token = args.prefix or f"benchmark/minio-tile-io/{time.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    started_at = utc_now()
    rows: list[dict[str, Any]] = []
    phases: list[dict[str, Any]] = []
    for round_number in range(1, args.rounds + 1):
        for count in counts:
            row, phase_rows = run_write_level(
                client,
                settings.bucket,
                token,
                sample_payloads[WRITE_OBJECT_TYPE],
                round_number,
                count,
                args.concurrency,
            )
            rows.append(row)
            phases.extend(phase_rows)
    for object_type in read_object_types:
        read_pool = candidates[object_type][:args.read_pool_size]
        for round_number in range(1, args.rounds + 1):
            for count in counts:
                row, phase_rows = run_read_level(
                    client,
                    read_pool,
                    object_type,
                    round_number,
                    count,
                    args.concurrency,
                )
                rows.append(row)
                phases.extend(phase_rows)
    finished_at = utc_now()
    summary = aggregate(rows)
    sample_metadata = {
        "write_entity_tile": {
            key: value for key, value in write_sample.items() if key not in {"bucket", "object_key"}
        } | {"fetch_sec": sample_fetch[WRITE_OBJECT_TYPE]},
        **{
            f"read_{object_type}": {
                "pool_size": min(args.read_pool_size, len(candidates[object_type])),
                "objects": [
                    {key: value for key, value in item.items() if key not in {"bucket", "object_key"}}
                    for item in candidates[object_type][:args.read_pool_size]
                ],
            }
            for object_type in read_object_types
        },
    }
    metadata = {
        "schema_version": "minio-tile-io-benchmark-v1",
        "started_at": started_at,
        "finished_at": finished_at,
        "source_root": str(source_root),
        "bucket": settings.bucket,
        "benchmark_prefix": f"s3://{settings.bucket}/{token}/",
        "write_object_types": write_object_types,
        "read_object_types": read_object_types,
        "tile_counts": counts,
        "rounds": args.rounds,
        "concurrency": args.concurrency,
        "cleanup": True,
        "samples": sample_metadata,
        "sample_fetch_sec": sample_fetch,
        "sample_count": len(rows),
        "valid_sample_count": sum(bool(row["valid"]) for row in rows),
    }
    sample_fields = [
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
    ] if rows else []
    summary_fields = list(summary[0].keys()) if summary else []
    phase_fields = ["operation", "object_type", "round", "tile_count", "phase", "elapsed_sec"]
    write_csv(output_dir / "minio_tile_io_samples.csv", rows, sample_fields)
    write_csv(output_dir / "minio_tile_io_summary.csv", summary, summary_fields)
    write_csv(output_dir / "minio_tile_io_phases.csv", phases, phase_fields)
    (output_dir / "minio_tile_io_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({
        "summary": str(output_dir / "minio_tile_io_summary.csv"),
        "samples": str(output_dir / "minio_tile_io_samples.csv"),
        "phases": str(output_dir / "minio_tile_io_phases.csv"),
        "metadata": str(output_dir / "minio_tile_io_metadata.json"),
        "sample_count": len(rows),
        "valid_sample_count": sum(bool(row["valid"]) for row in rows),
    }, ensure_ascii=False))
    return 0 if all(row["valid"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
