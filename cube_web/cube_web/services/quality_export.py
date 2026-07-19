from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Iterator
from datetime import UTC
from typing import Literal
from uuid import UUID

from cube_web.services.quality_contracts import ExportFormat, QualityError, QualityErrorFilter, QualityRun
from cube_web.services.quality_repository import (
    count_quality_errors,
    get_quality_run,
    iter_quality_errors,
    require_open_gauss_domain_store,
)

CSV_COLUMNS = (
    "quality_error_id",
    "quality_run_id",
    "rule_code",
    "scene_id",
    "scene_name",
    "source_asset_id",
    "band_code",
    "band_name",
    "tile_id",
    "index_id",
    "output_id",
    "row_number",
    "field",
    "error_code",
    "message",
    "context",
    "created_at",
)


def quality_export_filename(run: QualityRun, export_format: ExportFormat, filtered: bool) -> str:
    dataset_code = re.sub(r"[^A-Za-z0-9._-]", "_", run.dataset_code).strip("._") or "dataset"
    quality_time = (run.completed_at or run.started_at or run.created_at).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    filtered_suffix = "_filtered" if filtered else ""
    return f"{dataset_code}_{quality_time}_{run.quality_run_id}_errors{filtered_suffix}.{export_format}"


def csv_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    yield buffer.getvalue().encode("utf-8-sig")
    buffer.seek(0)
    buffer.truncate(0)
    for row in rows:
        payload = row.model_dump(mode="json")
        payload["context"] = json.dumps(payload["context"], ensure_ascii=False, separators=(",", ":"))
        for key, value in payload.items():
            if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
                payload[key] = "'" + value
        writer.writerow(payload)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)


def json_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    yield b"["
    first = True
    for row in rows:
        if not first:
            yield b","
        first = False
        yield json.dumps(row.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    yield b"]"


def stream_quality_errors(
    quality_run_id: UUID,
    filters: QualityErrorFilter,
    export_format: ExportFormat,
) -> tuple[Iterator[bytes], int, str, Literal["text/csv; charset=utf-8", "application/json"]]:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        run = get_quality_run(tx, quality_run_id=quality_run_id)
        total = count_quality_errors(tx, quality_run_id=quality_run_id, filters=filters)
    encoder = csv_chunks if export_format == "csv" else json_chunks
    media_type: Literal["text/csv; charset=utf-8", "application/json"] = "text/csv; charset=utf-8" if export_format == "csv" else "application/json"

    def stream() -> Iterator[bytes]:
        with store.transaction() as tx:
            yield from encoder(iter_quality_errors(tx, quality_run_id=quality_run_id, filters=filters))

    return stream(), total, quality_export_filename(run, export_format, filters.active()), media_type
