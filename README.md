# cube_encoder MVP

Geohash-first grid partition and space-time code engine with FastAPI + SDK layout.

中文介绍文档：`README.zh-CN.md`

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn grid_core.app.main:app --host 0.0.0.0 --port 50012 --reload
```

Open visual demo:

- `http://127.0.0.1:50012/v1/demo/map`

## API examples

```bash
curl -X POST http://127.0.0.1:50012/v1/grid/locate \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"point":[116.391,39.907]}'

curl -X POST http://127.0.0.1:50012/v1/code/st \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"space_code":"wtw3sjq","timestamp":"2026-03-09T15:30:00Z","time_granularity":"minute","version":"v1"}'
```

## Python SDK usage

```bash
pip install -e .
```

```python
from datetime import datetime, timezone

from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cell = sdk.locate(grid_type="geohash", level=7, point=[116.391, 39.907])
neighbors = sdk.neighbors(grid_type="geohash", code=cell.space_code, k=1)
st_code = sdk.generate_st_code(
    grid_type="geohash",
    level=7,
    space_code=cell.space_code,
    timestamp=datetime(2026, 3, 9, 15, 30, tzinfo=timezone.utc),
    time_granularity="minute",
    version="v1",
).st_code
```

## Test

```bash
pytest -q
```

## Performance Smoke

```bash
python -m grid_core.app.perf_smoke
```

## Spark Logical Partition (COG)

Run a complete local Spark flow (COG scan -> grid cover tasks -> on-demand COG window read -> space/time index -> parquet output):

```bash
scripts/run_spark_logical_partition.sh
```

Custom parameters:

```bash
GRID_TYPE=geohash GRID_LEVEL=5 COVER_MODE=intersect REPARTITION=4 MAX_CELLS_PER_ASSET=5000 scripts/run_spark_logical_partition.sh
```

Inspect output (`run_dir` is printed by job):

```bash
python grid_core/spark_jobs/inspect_partition_output.py --run-dir data/spark_output/logical_partition/run_YYYYMMDD_HHMMSS
```

## Development Docs

- Task log: `docs/DEVELOPMENT_LOG.md`
- Status & next plan: `docs/STATUS_AND_PLAN.md`
- Bug log: `docs/BUG_LOG.md`
- Process guideline: `docs/DOC_WORKFLOW.md`
- SDK release policy: `docs/SDK_RELEASE.md`
- Changelog: `CHANGELOG.md`

## MVP limits

- `geohash` supports locate/cover/topology base capabilities.
- `mgrs` supports first-phase locate + geometry reverse (`code_to_bbox/code_to_geometry`), basic topology (`neighbors/parent/children`), and `cover_mode=intersect/contain/minimal`.
- `isea4h` is now backed by Uber H3 for first-phase runnable capability (`locate/cover/topology`).
- `cover_mode=intersect/contain/minimal` implemented.
- `cover_mode=minimal` supports cross-level coarsening (response cells may include levels lower than request level).
- Frontend visualizer available at `/v1/demo/map` with API/SDK switch.
- Frontend visualizer supports `locate/cover/neighbors/parent/children` map rendering.
- Frontend visualizer supports drawing polygon/rectangle on map for `cover` preview.
- Topology now supports batch geometry API (`/v1/topology/geometries`) for faster visualization.
- CRS fixed to `EPSG:4326`.
