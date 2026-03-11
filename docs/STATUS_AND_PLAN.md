# Project Status And Plan

## Purpose
Single-page handoff for ongoing development:
- What is already done.
- What is in progress/next.
- What to execute in each upcoming task.

---

## Current Status (as of 2026-03-11)

### Implemented capabilities
- Multi-engine routing:
  - `geohash`
  - `mgrs`
  - `isea4h` (H3-backed)
- Grid APIs:
  - `/v1/grid/locate`
  - `/v1/grid/cover` (`intersect/contain/minimal`)
- Topology APIs:
  - `/v1/topology/neighbors`
  - `/v1/topology/parent`
  - `/v1/topology/children`
  - `/v1/topology/geometry`
  - `/v1/topology/geometries` (batch, for performance)
- ST Code APIs:
  - `/v1/code/st`
  - `/v1/code/st/batch`
  - `/v1/code/parse`
- Python SDK:
  - `pip install -e .`
  - `from grid_core.sdk import CubeEncoderSDK`
  - Full parity methods for grid/topology/ST-code core flows
- Demo frontend:
  - `/v1/demo/map`
  - API/SDK switch
  - locate/cover/neighbors/parent/children visualization
  - draw polygon/rectangle for cover preview
- Performance guard:
  - `python -m grid_core.app.perf_smoke`
  - CI `perf-smoke` job

### Quality baseline
- Latest full test result: `71 passed`
- Core docs maintained:
  - `docs/DEVELOPMENT_LOG.md`
  - `docs/BUG_LOG.md`
  - `docs/DOC_WORKFLOW.md`

---

## Next Planned Tasks

### TASK-0020 (Next): Frontend layer controls and rendering ergonomics
- Goal:
  - Improve large result-set usability and rendering stability.
- Planned changes:
  - Layer legend by grid type.
  - Opacity/line-width controls.
  - Result cap + paging/sampling indicator for topology rendering.
- Acceptance criteria:
  - Frontend remains mobile/desktop usable.
  - Demo tests extended for new controls.

### Completed this round
- `TASK-0019`: Geohash cover hot-path optimization (bbox pre-filter + prepared geometry + bbox reuse) completed.
- `TASK-0021`: MGRS `minimal` mode completed.
- `TASK-0022`: Perf trend visibility completed (JSON artifact export + CI upload + centralized thresholds).
- `TASK-0024`: SDK release pipeline completed (SemVer/changelog policy + CI package smoke job).

### TASK-0026: Minimal strategy precision tuning
- Goal:
  - Improve cross-level minimal output quality and predictability for complex boundaries.
- Planned changes:
  - Add optional policy knobs for merge aggressiveness.
  - Add golden test fixtures for mixed-level outputs.
- Acceptance criteria:
  - Invariant tests pass and mixed-level output remains deterministic.

---

## Execution Rule For Next Iterations
- For each task:
  1. Implement.
  2. Add/adjust strict tests.
  3. Run:
     - `python -m pytest -q tests`
     - `python -m grid_core.app.perf_smoke` (if perf-related)
  4. Update docs:
     - `docs/DEVELOPMENT_LOG.md`
     - `docs/STATUS_AND_PLAN.md` (if plan/status changes)
  5. Commit and push.
