# Development Task Log

## Purpose
Track every development task with scope, decisions, changes, and validation results.

## Entry Format
- Date: `YYYY-MM-DD`
- Task ID: `TASK-XXXX`
- Goal
- Scope
- Key Changes
- Validation
- Next

---

## 2026-03-09 | TASK-0001 | MVP scaffold (Geohash + API + SDK)
- Goal: Build first runnable MVP from `info.md`.
- Scope: Geohash engine, core models/services, FastAPI routers, tests.
- Key Changes:
  - Implemented geohash locate/cover/geometry/neighbors/parent/children.
  - Implemented ST code generate/parse.
  - Added unified request/response models and error handling.
- Validation:
  - `python -m pytest -q tests`
  - Result: passed.
- Next:
  - Add batch ST code generation.

## 2026-03-09 | TASK-0002 | Add bbox cover + topology parent/children API
- Goal: Close capability gap for bbox coverage input and topology API exposure.
- Scope: Request model, service routing, topology API, tests.
- Key Changes:
  - `CoverRequest` now accepts `bbox` input.
  - Added `/v1/topology/parent` and `/v1/topology/children`.
- Validation:
  - `python -m pytest -q tests`
  - Result: 9 passed.
- Next:
  - Add batch ST code endpoint.

## 2026-03-09 | TASK-0003 | Add batch ST code endpoint + doc mechanism
- Goal: Continue MVP by adding batch code generation and document workflow.
- Scope: code API/service/models/tests + docs standards.
- Key Changes:
  - Added `/v1/code/st/batch`.
  - Added `STCodeBatchGenerateRequest/Response`.
  - Added service method `batch_generate_st_codes`.
- Validation:
  - `python -m pytest -q tests`
  - Result: 11 passed.
- Next:
  - Keep this log updated for every task.
