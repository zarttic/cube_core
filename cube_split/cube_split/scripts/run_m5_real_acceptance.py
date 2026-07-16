"""Fail-closed launcher for the M5 OpenGauss/MinIO/Ray acceptance gate.

The two manifests intentionally live outside the repository.  The input
manifest has exactly the six immutable case IDs and a strict M2 request for
each.  The defect manifest has strict replacement requests for the two cases
that need a real execution/quality defect.  This module validates only
metadata and MinIO object existence; source bytes are read later, solely by
the production Ray source-cache path exercised by the real tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlparse

from cube_web.services.partition_contracts import StrictPartitionRequest, resolve_dataset_partition
from minio import Minio

from cube_split import runtime_config


@dataclass(frozen=True)
class RealAcceptanceCase:
    id: str


REAL_ACCEPTANCE_CASES = (
    RealAcceptanceCase("geohash_logical_single_dataset"),
    RealAcceptanceCase("mgrs_cross_zone_boundary_logical"),
    RealAcceptanceCase("isea4h_low_resolution_entity"),
    RealAcceptanceCase("batch_two_datasets_sibling_partial_failure"),
    RealAcceptanceCase("quality_fail_complete_exports"),
    RealAcceptanceCase("pass_warn_publish_withdraw_reconciliation"),
)
CASE_IDS = tuple(case.id for case in REAL_ACCEPTANCE_CASES)
DEFECT_CASE_IDS = frozenset({
    "batch_two_datasets_sibling_partial_failure",
    "quality_fail_complete_exports",
})
REQUIRED_ENVIRONMENT = (
    "CUBE_WEB_POSTGRES_DSN",
    "CUBE_WEB_RAY_ADDRESS",
    "CUBE_WEB_MINIO_ENDPOINT",
    "CUBE_WEB_MINIO_ACCESS_KEY",
    "CUBE_WEB_MINIO_SECRET_KEY",
    "CUBE_WEB_MINIO_BUCKET",
)
FORBIDDEN_MANIFEST_TOKENS = frozenset({
    "convert_asset_to_cog",
    "cog_workers",
    "cog_overwrite",
    "conversion_timing",
    "source_upload",
    "reproject",
    "reprojection",
    "grid_level_mode",
    "generated",
    "synthetic",
    "credential",
    "password",
    "secret",
    "access_key",
})
_ABSOLUTE_PATH = re.compile(r"(?<![A-Za-z0-9_:/])/(?:[^\s'\"\\]+/?)+")
_SENSITIVE_VALUE = re.compile(r"(?i)(credential|password|secret|access[_-]?key|dsn|synthetic|generated|convert_asset_to_cog|cog_workers|cog_overwrite|reproject(?:ion)?)")
_DSN_CREDENTIALS = re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^:/@\s]+:)[^@/\s]+@")


class ManifestError(ValueError):
    """Raised when a real-gate manifest cannot prove the strict contract."""


def _redact_text(value: str) -> str:
    value = _DSN_CREDENTIALS.sub(r"\1<redacted>@", value)
    value = re.sub(r"(?i)(password|secret|access[_-]?key)=[^\s&]+", r"\1=<redacted>", value)
    return _ABSOLUTE_PATH.sub("<absolute-path>", value)


def redact(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    if isinstance(value, dict):
        return {str(key): "<redacted>" if _SENSITIVE_VALUE.search(str(key)) else redact(item) for key, item in value.items()}
    return value


def _load_json(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ManifestError(f"{label} does not exist or is not a file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"{label} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ManifestError(f"{label} must be a JSON object")
    return payload


def _reject_forbidden(value: Any, *, path: str = "manifest") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in FORBIDDEN_MANIFEST_TOKENS or "credential" in normalized:
                raise ManifestError(f"{path} contains forbidden field {key!r}")
            _reject_forbidden(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_forbidden(item, path=f"{path}[{index}]")
    elif isinstance(value, str) and _SENSITIVE_VALUE.search(value):
        raise ManifestError(f"{path} contains a forbidden credential, synthetic, or conversion value")


def _strict_case_map(payload: Mapping[str, Any], *, label: str, expected_ids: Sequence[str]) -> dict[str, StrictPartitionRequest]:
    if set(payload) != {"cases"}:
        raise ManifestError(f"{label} must contain only a cases array")
    raw_cases = payload["cases"]
    if not isinstance(raw_cases, list):
        raise ManifestError(f"{label}.cases must be an array")
    requests: dict[str, StrictPartitionRequest] = {}
    for index, raw_case in enumerate(raw_cases):
        if not isinstance(raw_case, dict) or set(raw_case) != {"id", "request"}:
            raise ManifestError(f"{label}.cases[{index}] must contain exactly id and request")
        case_id = raw_case["id"]
        if not isinstance(case_id, str) or not case_id:
            raise ManifestError(f"{label}.cases[{index}].id must be a nonempty string")
        _reject_forbidden(raw_case["request"], path=f"{label}.cases[{index}].request")
        try:
            request = StrictPartitionRequest.model_validate(raw_case["request"])
        except Exception as exc:
            raise ManifestError(f"{label} case {case_id!r} is not a StrictPartitionRequest: {exc}") from exc
        serialized = request.model_dump(mode="json")
        if "grid_level" in serialized or "grid_level_mode" in serialized:
            raise ManifestError(f"{label} case {case_id!r} serializes forbidden request-level fields")
        if case_id in requests:
            raise ManifestError(f"{label} duplicates case ID {case_id!r}")
        requests[case_id] = request
    if tuple(requests) != tuple(expected_ids):
        raise ManifestError(f"{label} case IDs must be exactly {list(expected_ids)!r} in immutable order")
    return requests


def _uri_key(uri: str, bucket: str, *, suffixes: tuple[str, ...], label: str) -> str:
    parsed = urlparse(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or not parsed.path.lstrip("/")
        or not parsed.path.lower().endswith(suffixes)
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ManifestError(f"strict {label} URI must be a supported object in the configured MinIO bucket")
    return unquote(parsed.path).lstrip("/")


def _redacted_source(uri: str) -> str:
    parsed = urlparse(uri)
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()[:16]
    return f"s3://{parsed.netloc}/<source:{digest}>"


def validate_real_manifests(
    *,
    input_manifest: Path,
    defect_manifest: Path,
    environment: Mapping[str, str] | None = None,
    minio_client: Any | None = None,
) -> dict[str, Any]:
    """Validate two external manifests and stat every loader-owned source object."""
    environment = _resolved_environment(environment)
    missing = [name for name in REQUIRED_ENVIRONMENT if not str(environment.get(name, "")).strip()]
    if missing:
        raise ManifestError("missing required environment: " + ", ".join(missing))
    input_manifest = input_manifest.resolve()
    defect_manifest = defect_manifest.resolve()
    repository = Path(__file__).resolve().parents[3]
    for label, path in (("CUBE_M5_REAL_INPUT_MANIFEST", input_manifest), ("CUBE_M5_REAL_DEFECT_MANIFEST", defect_manifest)):
        if path == repository or repository in path.parents:
            raise ManifestError(f"{label} must point outside the repository")
    inputs = _strict_case_map(_load_json(input_manifest, "input manifest"), label="input manifest", expected_ids=CASE_IDS)
    defects = _strict_case_map(
        _load_json(defect_manifest, "defect manifest"), label="defect manifest", expected_ids=tuple(sorted(DEFECT_CASE_IDS))
    )
    if set(defects) != DEFECT_CASE_IDS:
        raise ManifestError("defect manifest must only cover the two declared defect cases")
    for case_id, request in inputs.items():
        if case_id == "geohash_logical_single_dataset" and (request.grid_type, request.partition_method) != ("geohash", "logical"):
            raise ManifestError("geohash scenario must be logical Geohash")
        if case_id == "mgrs_cross_zone_boundary_logical" and (request.grid_type, request.partition_method) != ("mgrs", "logical"):
            raise ManifestError("MGRS scenario must be logical MGRS")
        if case_id == "isea4h_low_resolution_entity" and (request.grid_type, request.partition_method, request.requested_grid_level) != ("isea4h", "entity", 1):
            raise ManifestError("ISEA4H scenario must be entity resolution 1")
    geohash_datasets = inputs["geohash_logical_single_dataset"].datasets
    if len(geohash_datasets) != 1 or geohash_datasets[0].data_type != "optical":
        raise ManifestError("Geohash scenario must contain one optical dataset")
    mgrs_datasets = inputs["mgrs_cross_zone_boundary_logical"].datasets
    expected_domains = mgrs_datasets[0].attributes.get("m5_expected_mgrs_domains") if len(mgrs_datasets) == 1 else None
    if not isinstance(expected_domains, list) or len(expected_domains) < 2 or not all(str(item).startswith("utm-") for item in expected_domains):
        raise ManifestError("MGRS scenario must declare two reviewed UTM topology domains")
    partial_datasets = defects["batch_two_datasets_sibling_partial_failure"].datasets
    if len(partial_datasets) != 2 or {dataset.data_type for dataset in partial_datasets} != {"optical", "radar"}:
        raise ManifestError("partial-failure defect scenario must contain one optical and one radar dataset")
    partial_request = defects["batch_two_datasets_sibling_partial_failure"]
    partial_configs = {resolve_dataset_partition(partial_request, dataset).grid_type for dataset in partial_datasets}
    if len(partial_configs) != 2:
        raise ManifestError("partial-failure mixed scenario must use different dataset grid types")
    quality_datasets = defects["quality_fail_complete_exports"].datasets
    if len(quality_datasets) != 1 or quality_datasets[0].data_type != "carbon":
        raise ManifestError("quality defect scenario must contain one carbon dataset")
    publication_datasets = inputs["pass_warn_publish_withdraw_reconciliation"].datasets
    if len(publication_datasets) != 2 or {dataset.data_type for dataset in publication_datasets} != {"optical"}:
        raise ManifestError("publication scenario must contain two optical datasets")
    if not any(asset.attributes.get("quality_metadata_defects") for dataset in publication_datasets for asset in dataset.assets):
        raise ManifestError("publication scenario must include a manifest-declared Warn dataset")
    client = minio_client or Minio(
        str(environment["CUBE_WEB_MINIO_ENDPOINT"]),
        access_key=str(environment["CUBE_WEB_MINIO_ACCESS_KEY"]),
        secret_key=str(environment["CUBE_WEB_MINIO_SECRET_KEY"]),
        secure=False,
    )
    bucket = str(environment["CUBE_WEB_MINIO_BUCKET"])
    if not client.bucket_exists(bucket):
        raise ManifestError("configured MinIO bucket does not exist")
    sources: list[str] = []
    for request in (*inputs.values(), *defects.values()):
        for dataset in request.datasets:
            for asset in dataset.assets:
                if dataset.data_type == "carbon":
                    uri = str(asset.source_uri)
                    suffixes = (".nc", ".nc4") if asset.source_format == "netcdf" else (".h5", ".hdf", ".hdf5")
                    key = _uri_key(uri, bucket, suffixes=suffixes, label=f"carbon {asset.source_format}")
                else:
                    uri = str(asset.cog_uri)
                    key = _uri_key(uri, bucket, suffixes=(".tif", ".tiff"), label="COG")
                stat = client.stat_object(bucket, key)
                if int(getattr(stat, "size", 0) or 0) <= 0:
                    raise ManifestError("strict source object has no content")
                sources.append(_redacted_source(uri))
    return {
        "input_manifest_digest": hashlib.sha256(input_manifest.read_bytes()).hexdigest(),
        "defect_manifest_digest": hashlib.sha256(defect_manifest.read_bytes()).hexdigest(),
        "case_ids": list(CASE_IDS),
        "input_requests": {case_id: request.model_dump(mode="json") for case_id, request in inputs.items()},
        "defect_requests": {case_id: request.model_dump(mode="json") for case_id, request in defects.items()},
        "sources": sorted(set(sources)),
    }


def _resolved_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    """Resolve CUBE_WEB_ENV_FILE via the existing runtime-config contract."""
    values = dict(os.environ if environment is None else environment)
    for name in REQUIRED_ENVIRONMENT:
        if not str(values.get(name, "")).strip():
            values[name] = runtime_config.env_text(name)
    return values


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False) as output:
        json.dump(redact(dict(payload)), output, ensure_ascii=True, sort_keys=True, indent=2)
        output.write("\n")
        temporary = Path(output.name)
    temporary.replace(path)


def _gate_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        "cube_split/tests/real/test_m5_real_acceptance.py",
        "-v",
        "-m",
        "m5_real",
        "-rs",
    ]


def _worktree_pythonpath() -> str:
    root = Path(__file__).resolve().parents[3]
    return os.pathsep.join(str(root / name) for name in ("cube_encoder", "cube_split", "cube_web"))


def _assert_pytest_six(transcript: str) -> None:
    forbidden = re.search(r"\b(skipped|SKIPPED|deselected|xfailed|XFAIL|xpassed|XPASS)\b", transcript)
    if forbidden:
        raise RuntimeError(f"M5 real pytest gate reported forbidden state: {forbidden.group(0)}")
    collected = re.findall(r"collected\s+(\d+)\s+items?", transcript)
    passed = re.findall(r"(?:^|\n).*?\b(\d+)\s+passed\b", transcript)
    if not collected or int(collected[-1]) != len(CASE_IDS) or not passed or int(passed[-1]) != len(CASE_IDS):
        raise RuntimeError("M5 real pytest gate must collect and pass exactly six tests")


def _read_scenario_summary(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"real test did not produce a valid scenario summary: {exc}") from exc
    scenarios = payload.get("scenarios") if isinstance(payload, dict) else None
    if not isinstance(scenarios, list):
        raise ManifestError("real test summary does not contain scenarios")
    if [item.get("id") for item in scenarios if isinstance(item, dict)] != list(CASE_IDS):
        raise ManifestError("real test summary does not contain exactly the immutable six scenarios")
    if any(not isinstance(item, dict) or item.get("status") != "passed" for item in scenarios):
        raise ManifestError("real test summary contains a non-passing scenario")
    return scenarios


def run_gate(*, summary_path: Path, input_manifest: Path, defect_manifest: Path, environment: Mapping[str, str] | None = None) -> int:
    environment = _resolved_environment(environment)
    metadata = validate_real_manifests(input_manifest=input_manifest, defect_manifest=defect_manifest, environment=environment)
    with tempfile.TemporaryDirectory(prefix="cube-m5-real-") as directory:
        scenario_path = Path(directory) / "scenarios.json"
        environment.update(
            {
                "CUBE_M5_REAL_INPUT_MANIFEST": str(input_manifest.resolve()),
                "CUBE_M5_REAL_DEFECT_MANIFEST": str(defect_manifest.resolve()),
                "CUBE_M5_REAL_SCENARIO_SUMMARY": str(scenario_path),
                "RAY_ADDRESS": environment["CUBE_WEB_RAY_ADDRESS"],
                "CUBE_WEB_AUTH_REQUIRED": "0",
                "PYTHONPATH": _worktree_pythonpath(),
            }
        )
        completed = subprocess.run(_gate_command(), env=environment, text=True, capture_output=True, check=False)
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        transcript = completed.stdout + completed.stderr
        if completed.returncode:
            raise RuntimeError(f"M5 real pytest gate failed with exit status {completed.returncode}")
        _assert_pytest_six(transcript)
        scenarios = _read_scenario_summary(scenario_path)
    payload = {
        "m5_gate_status": "PASS",
        "scenario_count": len(scenarios),
        "passed": len(scenarios),
        "failed": 0,
        "skipped": 0,
        "deselected": 0,
        "cases": scenarios,
        "manifest": {key: metadata[key] for key in ("input_manifest_digest", "defect_manifest_digest", "sources")},
    }
    _atomic_json(summary_path, payload)
    print("M5_GATE_STATUS=PASS")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run exactly six real M5 OpenGauss/MinIO/Ray acceptance scenarios")
    parser.add_argument("--summary-path", required=True, type=Path)
    parser.add_argument("--input-manifest", type=Path)
    parser.add_argument("--defect-manifest", type=Path)
    parser.add_argument("--asset-manifest", type=Path, help="Compatibility alias for --input-manifest")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    input_manifest = args.input_manifest or args.asset_manifest or os.getenv("CUBE_M5_REAL_INPUT_MANIFEST")
    defect_manifest = args.defect_manifest or os.getenv("CUBE_M5_REAL_DEFECT_MANIFEST")
    if not input_manifest or not defect_manifest:
        raise SystemExit("CUBE_M5_REAL_INPUT_MANIFEST and CUBE_M5_REAL_DEFECT_MANIFEST are required")
    try:
        return run_gate(summary_path=args.summary_path, input_manifest=Path(input_manifest), defect_manifest=Path(defect_manifest))
    except Exception as exc:
        print(f"M5 real acceptance failed: {redact(str(exc))}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
