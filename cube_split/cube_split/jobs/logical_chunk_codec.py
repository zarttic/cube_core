"""Stable binary encoding for immutable logical partition chunks."""

from __future__ import annotations

import gzip
import json
from hashlib import sha256
from typing import Any


def compress_logical_chunk(content: bytes) -> bytes:
    """Compress chunk rows deterministically so retries remain idempotent."""
    return gzip.compress(content, mtime=0)


def serialize_logical_chunk_rows(rows: list[dict[str, Any]]) -> bytes:
    """Encode logical rows in a stable order before immutable object upload."""
    ordered_rows = sorted(
        rows,
        key=lambda item: (str(item["kind"]), str(item["row"]["output_id"])),
    )
    return b"".join(
        json.dumps(row, separators=(",", ":"), ensure_ascii=False, sort_keys=True).encode()
        + b"\n"
        for row in ordered_rows
    )


def logical_chunk_id(
    *, dataset_id: str, output_version: str, shard_id: str, bands: list[dict[str, Any]],
) -> str:
    """Identify one immutable logical chunk, including its selected bands."""
    band_identity = "\0".join(
        sorted(
            f"{band['source_asset_id']}:{band['band_code']}:{(band.get('attributes') or {}).get('band_unit_id') or ''}"
            for band in bands
        )
    )
    return sha256(f"{dataset_id}\0{output_version}\0{shard_id}\0{band_identity}".encode()).hexdigest()[:32]
