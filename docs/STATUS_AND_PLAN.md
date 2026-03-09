# Project Status And Plan

## Purpose
Single-page handoff for ongoing development:
- What is already done.
- What is in progress/next.
- What to execute in each upcoming task.

---

## Current Status (as of 2026-03-09)

### Implemented capabilities
- Multi-engine routing:
  - `geohash`
  - `mgrs`
  - `isea4h` (H3-backed)
- Grid APIs:
  - `/v1/grid/locate`
  - `/v1/grid/cover` (`intersect/contain/minimal`, engine-dependent)
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
- Demo frontend:
  - `/v1/demo/map`
  - API/SDK switch
  - locate/cover/neighbors/parent/children visualization
  - draw polygon/rectangle for cover preview
- Performance guard:
  - `python -m grid_core.app.perf_smoke`
  - CI `perf-smoke` job

### Quality baseline
- Latest full test result: `62 passed`
- Core docs maintained:
  - `docs/DEVELOPMENT_LOG.md`
  - `docs/BUG_LOG.md`
  - `docs/DOC_WORKFLOW.md`

---

## Next Planned Tasks

### TASK-0019 (Next): Geohash cover hot-path optimization
- Goal:
  - Reduce CPU and allocations for large-area geohash cover.
- Planned changes:
  - Cache/reuse decoded bbox and polygon objects in geohash cover loops.
  - Add fast pre-filter before shapely intersection where possible.
  - Add micro-benchmark case in `perf_smoke`.
- Acceptance criteria:
  - No API behavior change.
  - `python -m pytest -q tests` passes.
  - `python -m grid_core.app.perf_smoke` passes.

### TASK-0020: Frontend layer controls and rendering ergonomics
- Goal:
  - Improve large result-set usability and rendering stability.
- Planned changes:
  - Layer legend by grid type.
  - Opacity/line-width controls.
  - Result cap + paging/sampling indicator for topology rendering.
- Acceptance criteria:
  - Frontend remains mobile/desktop usable.
  - Demo tests extended for new controls.

### TASK-0021: MGRS `minimal` cover mode
- Goal:
  - Close the remaining MGRS cover-mode gap.
- Planned changes:
  - Implement `minimal` behavior and document semantics.
  - Add engine/API tests to enforce subset/consistency invariants.
- Acceptance criteria:
  - Existing `intersect/contain` behavior unchanged.
  - New tests cover edge cases and pass.

### TASK-0022: Perf trend visibility in CI artifacts
- Goal:
  - Keep performance evolution visible over time.
- Planned changes:
  - Export perf smoke JSON artifact in CI.
  - Keep threshold env vars centralized and documented.
- Acceptance criteria:
  - PR can inspect perf output artifacts directly.

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
