import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from grid_core.ingest.ray_ingest_job import (
    build_cube_fact_records,
    build_raw_asset_records,
    ensure_tables,
    load_rows,
    run_ingest,
)


def _sample_row(scene_id: str, acq_time: str, space_code: str = "wtw3", band: str = "b04") -> dict:
    return {
        "scene_id": scene_id,
        "band": band,
        "asset_path": f"/tmp/{scene_id}_{band}.TIF",
        "acq_time": acq_time,
        "grid_type": "geohash",
        "grid_level": 7,
        "space_code": space_code,
        "space_code_prefix": "wtw",
        "st_code": f"gh:7:{space_code}:20260421:v1",
        "time_bucket": "20260421",
        "cover_mode": "intersect",
        "cell_min_lon": 116.1,
        "cell_min_lat": 39.8,
        "cell_max_lon": 116.2,
        "cell_max_lat": 39.9,
        "window_col_off": 0,
        "window_row_off": 0,
        "window_width": 256,
        "window_height": 256,
        "intersect_min_lon": 116.1,
        "intersect_min_lat": 39.8,
        "intersect_max_lon": 116.2,
        "intersect_max_lat": 39.9,
        "sample_mean_band1": 1.0,
    }


def test_build_raw_asset_records_deduplicates_by_scene_band():
    rows = [
        _sample_row("S1", "2026-04-21T00:00:00Z"),
        _sample_row("S1", "2026-04-21T01:00:00Z"),
        _sample_row("S2", "2026-04-21T02:00:00Z"),
    ]
    records = build_raw_asset_records(
        rows=rows,
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        run_id="job-1",
    )
    assert len(records) == 2
    by_scene = {row.scene_id: row for row in records}
    assert by_scene["S1"].acq_time == "2026-04-21T01:00:00Z"


def test_build_cube_fact_records_resolves_conflict_with_latest_scene():
    rows = [
        _sample_row("S_OLD", "2026-04-21T00:00:00Z", space_code="wtw3"),
        _sample_row("S_NEW", "2026-04-21T06:00:00Z", space_code="wtw3"),
    ]
    facts = build_cube_fact_records(
        rows=rows,
        cube_version="v1",
        run_id="job-1",
        quality_rule="best_quality_wins",
    )
    assert len(facts) == 1
    fact = facts[0]
    provenance = json.loads(fact.provenance_json)
    assert provenance["winner_scene_id"] == "S_NEW"
    assert sorted(provenance["candidate_scene_ids"]) == ["S_NEW", "S_OLD"]
    assert fact.source_scene_count == 2


def test_run_ingest_creates_and_upserts_tables(tmp_path: Path):
    run_dir = tmp_path / "run_001"
    run_dir.mkdir(parents=True)
    rows_path = run_dir / "index_rows.jsonl"
    rows = [
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wtw3", band="b04"),
        _sample_row("S2", "2026-04-21T05:00:00Z", space_code="wtw3", band="b04"),
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wtw4", band="b04"),
    ]
    with rows_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    db_path = tmp_path / "ingest.db"

    args = SimpleNamespace(
        run_dir=str(run_dir),
        db_path=str(db_path),
        job_id="job-001",
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        cube_version="v1",
        quality_rule="best_quality_wins",
    )

    stats = run_ingest(args)
    assert stats["input_rows"] == 3
    assert stats["raw_asset_rows"] == 2
    assert stats["cube_fact_rows"] == 2

    conn = sqlite3.connect(str(db_path))
    try:
        raw_count = conn.execute("SELECT COUNT(*) FROM rs_raw_scene_asset").fetchone()[0]
        cube_count = conn.execute("SELECT COUNT(*) FROM rs_cube_cell_fact").fetchone()[0]
        job = conn.execute("SELECT status FROM rs_ingest_job WHERE job_id = ?", ("job-001",)).fetchone()
    finally:
        conn.close()

    assert raw_count == 2
    assert cube_count == 2
    assert job[0] == "succeeded"


def test_load_rows_rejects_empty_file(tmp_path: Path):
    run_dir = tmp_path / "run_empty"
    run_dir.mkdir()
    rows_path = run_dir / "index_rows.jsonl"
    rows_path.write_text("", encoding="utf-8")
    try:
        load_rows(rows_path)
    except RuntimeError as exc:
        assert "No rows found" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for empty rows file")


def test_ensure_tables_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "ingest.db"
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_tables(conn)
        ensure_tables(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'rs_%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"rs_raw_scene_asset", "rs_cube_cell_fact", "rs_ingest_job"}.issubset(tables)
