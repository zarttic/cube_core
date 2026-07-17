#!/usr/bin/env python3
"""Build an M6 mock manifest from read-only MinIO metadata and run its gate."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[2]
for package_root in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from cube_split import runtime_config
from cube_web.acceptance.m6_mock_data import (
    DATA_TYPES,
    build_mock_manifest,
    collect_source_snapshot,
    ensure_tmp_path,
)


def _explicit_sources() -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for data_type in DATA_TYPES:
        raw = os.getenv(f"CUBE_M6_{data_type.upper()}_SOURCE_URIS", "")
        uris = [value.strip() for value in raw.split(",") if value.strip()]
        if uris:
            values[data_type] = uris
    return values


def _write_manifest(manifest: dict, requested_path: str | None) -> Path:
    if requested_path:
        path = ensure_tmp_path(Path(requested_path))
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        directory = Path(tempfile.mkdtemp(prefix="cube-m6-acceptance-", dir="/tmp"))
        path = directory / "manifest.json"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
    return path


EXPECTED_TEST_COUNT = 9


def _run_gate(manifest_path: Path) -> None:
    environment = os.environ.copy()
    environment["CUBE_M6_MOCK_MANIFEST"] = str(manifest_path)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = os.pathsep.join(
        str(path) for path in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web")
    )
    command = [
        sys.executable,
        "-m",
        "pytest",
        "cube_web/tests/test_m6_mock_integration.py",
        "-v",
        "-rs",
        "-p",
        "no:cacheprovider",
        f"--junitxml={manifest_path.parent / 'pytest-results.xml'}",
    ]
    completed = subprocess.run(command, cwd=REPO_ROOT, env=environment, text=True, capture_output=True)
    print(completed.stdout, end="")
    print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode:
        raise RuntimeError("M6 mock acceptance gate failed")
    root = ET.parse(manifest_path.parent / "pytest-results.xml").getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals = {
        name: sum(int(suite.attrib.get(name, "0")) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }
    if totals != {"tests": EXPECTED_TEST_COUNT, "failures": 0, "errors": 0, "skipped": 0}:
        raise RuntimeError(f"M6 mock acceptance executed an unexpected test matrix: {totals}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="Manifest path under /tmp; a unique path is used by default")
    parser.add_argument("--prepare-only", action="store_true", help="Only list/stat sources and write the manifest")
    args = parser.parse_args(argv)

    settings = runtime_config.minio_settings()
    missing = [
        name
        for name, value in (
            ("CUBE_WEB_MINIO_ENDPOINT", settings.endpoint),
            ("CUBE_WEB_MINIO_ACCESS_KEY", settings.access_key),
            ("CUBE_WEB_MINIO_SECRET_KEY", settings.secret_key),
            ("CUBE_WEB_MINIO_BUCKET", settings.bucket),
        )
        if not value
    ]
    if missing:
        raise RuntimeError("M6 MinIO source gate missing runtime configuration: " + ", ".join(missing))

    from minio import Minio

    client = Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )
    snapshot = collect_source_snapshot(
        client,
        bucket=settings.bucket,
        explicit_uris=_explicit_sources(),
    )
    manifest = build_mock_manifest(snapshot)
    path = _write_manifest(manifest, args.output)
    if not args.prepare_only:
        _run_gate(path)
    print(
        json.dumps(
            {
                "gate": "m6-mock-acceptance",
                "status": "prepared" if args.prepare_only else "passed",
                "manifest": str(path),
                "source_counts": {key: len(value) for key, value in snapshot.items()},
                "scenarios": len(manifest["expected_coverage"]),
                "skipped": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
