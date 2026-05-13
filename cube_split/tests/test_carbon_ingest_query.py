import json
import sys
from pathlib import Path
from types import SimpleNamespace

from cube_split.ingest.carbon_ingest_job import run_carbon_ingest
from cube_split.read.carbon_query import summarize_xco2


def _carbon_row(observation_id: str) -> dict:
    return {
        "data_type": "carbon_satellite",
        "satellite": "OCO2",
        "product_type": "xco2",
        "observation_id": observation_id,
        "acq_time": "2020-12-31T00:01:06.700000Z",
        "time_bucket": "20201231",
        "grid_type": "geohash",
        "grid_level": 7,
        "space_code": "7d9bc",
        "st_code": "gh:7:7d9bc:20201231:v1",
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

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
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

        def __exit__(self, exc_type, exc, tb):
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

    stats = run_carbon_ingest(
        SimpleNamespace(
            run_dir=str(rows_path.parent),
            rows_path="",
            job_id="carbon-job-postgres",
            cube_version="v1",
            metadata_backend="postgres",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:55432/cube",
        )
    )

    assert stats["carbon_fact_rows"] == 1
    assert stats["metadata_backend"] == "postgres"
    assert ("connect", "postgresql://postgres:postgres@127.0.0.1:55432/cube") in calls
    assert ("executemany", 1) in calls


def test_summarize_xco2_reports_count_and_range():
    summary = summarize_xco2([{"xco2": 416.0}, {"xco2": 418.0}])

    assert summary == {
        "count": 2,
        "xco2_min": 416.0,
        "xco2_max": 418.0,
        "xco2_avg": 417.0,
    }
