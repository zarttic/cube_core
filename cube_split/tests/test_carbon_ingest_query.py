from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import cube_split.ingest.carbon_ingest_job as carbon_ingest_job
from cube_split.ingest.carbon_ingest_job import run_carbon_ingest
from cube_split.read.carbon_query import _parse_args, summarize_xco2


def _carbon_row(observation_id: str) -> dict:
    return {
        "data_type": "carbon",
        "satellite": "OCO2",
        "product_type": "xco2",
        "observation_id": observation_id,
        "acq_time": "2020-12-31T00:01:06.700000Z",
        "time_bucket": "20201231",
        "grid_type": "isea4h",
        "grid_level": 5,
        "space_code": "85230a2ffffffff",
        "st_code": "hx:5:85230a2ffffffff:20201231",
        "xco2": 417.384,
        "quality_flag": "1",
        "center_lon": -167.413,
        "center_lat": 41.1686,
        "footprint_geojson": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-167.416, 41.1673],
                    [-167.424, 41.1848],
                    [-167.409, 41.1699],
                    [-167.402, 41.1524],
                    [-167.416, 41.1673],
                ]
            ],
        },
        "source_uri": "s3://cube/carbon/raw/oco2.nc4",
        "source_index": 0,
        "metadata_json": json.dumps({"source_format": "oco2_lite_nc4"}),
    }


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_run_carbon_ingest_uses_postgres_backend(monkeypatch, tmp_path: Path):
    rows_path = tmp_path / "run_001" / "carbon_observation_rows.jsonl"
    _write_rows(rows_path, [_carbon_row("snd-postgres")])
    calls: list[tuple[str, object]] = []
    captured: list[carbon_ingest_job.TileProbeMetric] = []

    def fake_report_tile_metrics(metrics):
        captured.extend(list(metrics))

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def execute(self, sql, params=None):
            _ = params
            calls.append(("execute", sql))
            if "SELECT retry_count" in sql:
                self._row = None

        def executemany(self, sql, values):
            calls.append(("executemany", len(values)))

        def fetchone(self):
            return getattr(self, "_row", None)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append(("commit", None))

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            calls.append(("connect", dsn))
            return FakeConnection()

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    monkeypatch.setattr(carbon_ingest_job, "report_tile_metrics", fake_report_tile_metrics)

    stats = run_carbon_ingest(
        SimpleNamespace(
            run_dir=str(rows_path.parent),
            rows_path="",
            job_id="carbon-job-postgres",
            cube_version="v1",
            metadata_backend="postgres",
            postgres_dsn="postgresql://test_user:test_password@10.3.100.180:15400/postgres",
        )
    )

    assert stats["carbon_fact_rows"] == 1
    assert stats["metadata_backend"] == "postgres"
    assert ("connect", "postgresql://test_user:test_password@10.3.100.180:15400/postgres") in calls
    assert ("executemany", 1) in calls
    assert len(captured) == 1
    assert captured[0].task_name == "cube.partition.carbon.ingest"
    assert captured[0].method_name == "merge.rs_carbon_observation_fact"
    assert captured[0].attributes["cube.observation_id"] == "snd-postgres"
    assert captured[0].attributes["cube.space_code"] == "85230a2ffffffff"
    assert captured[0].attributes["cube.target_table"] == "rs_carbon_observation_fact"


def test_summarize_xco2_reports_count_and_range():
    summary = summarize_xco2([{"xco2": 416.0}, {"xco2": 418.0}])

    assert summary == {
        "count": 2,
        "xco2_min": 416.0,
        "xco2_max": 418.0,
        "xco2_avg": 417.0,
    }


def test_carbon_query_defaults_match_carbon_partition_grid(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "carbon_query",
            "--bbox",
            "-168.0",
            "40.5",
            "-166.5",
            "42.0",
            "--time-start",
            "20201231",
            "--time-end",
            "20201231",
        ],
    )

    args = _parse_args()

    assert args.grid_type == "isea4h"
    assert args.grid_level == 5
