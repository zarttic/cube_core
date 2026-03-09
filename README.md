# cube_encoder MVP

Geohash-first grid partition and space-time code engine with FastAPI + SDK layout.

中文介绍文档：`README.zh-CN.md`

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn grid_core.app.main:app --reload
```

## API examples

```bash
curl -X POST http://127.0.0.1:8000/v1/grid/locate \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"point":[116.391,39.907]}'

curl -X POST http://127.0.0.1:8000/v1/code/st \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"space_code":"wtw3sjq","timestamp":"2026-03-09T15:30:00Z","time_granularity":"minute","version":"v1"}'
```

## Test

```bash
pytest -q
```

## Development Docs

- Task log: `docs/DEVELOPMENT_LOG.md`
- Bug log: `docs/BUG_LOG.md`
- Process guideline: `docs/DOC_WORKFLOW.md`

## MVP limits

- `geohash` supports locate/cover/topology base capabilities.
- `mgrs` supports first-phase locate + geometry reverse (`code_to_bbox/code_to_geometry`) and basic topology (`neighbors/parent/children`).
- `isea4h` is routing-ready with explicit not-implemented responses.
- `cover_mode=intersect/contain/minimal` implemented.
- CRS fixed to `EPSG:4326`.
