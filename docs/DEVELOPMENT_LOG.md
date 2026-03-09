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

## 2026-03-09 | TASK-0004 | Implement geohash cover_mode contain/minimal
- Goal: Close `grid cover` mode gap for MVP by supporting `contain` and `minimal`.
- Scope: geohash engine cover logic, unit tests, README/doc sync.
- Key Changes:
  - Updated `GeohashEngine.cover_geometry` to support `intersect/contain/minimal`.
  - Added deterministic de-duplication and sorted output for coverage cell codes.
  - Added tests for `contain` and `minimal` behavior in `tests/test_geohash_engine.py`.
  - Updated README statements to reflect new cover mode support.
- Validation:
  - `python -m pytest -q tests`
  - Result: 13 passed.
- Next:
  - Improve `minimal` with cross-level optimization (currently same-level minimal candidate set).

## 2026-03-09 | TASK-0005 | Improve boundary stability for geohash cover
- Goal: Improve robustness for boundary-heavy coverage cases (dateline crossing and polar bbox).
- Scope: geometry bbox conversion, geohash cover candidate scan, boundary tests.
- Key Changes:
  - Updated `bbox_to_polygon` to validate ranges and support dateline-crossing bbox via `MultiPolygon` split.
  - Optimized geohash cover candidate generation to build candidate indices per geometry part instead of single global bounds scan.
  - Added tests for dateline-crossing bbox, polar bbox, and geometry conversion behavior.
- Validation:
  - `python -m pytest -q tests`
  - Result: 18 passed.
- Next:
  - Start `TASK-0006`: introduce engine registry/routing skeleton for multi-grid extension.

## 2026-03-09 | TASK-0006 | Add multi-engine routing skeleton
- Goal: Refactor service layer to route by `grid_type` and prepare MGRS/ISEA4H phased integration.
- Scope: enums, engine registry, placeholder engines, service routing, tests.
- Key Changes:
  - Added `GridType.MGRS` and `GridType.ISEA4H`.
  - Added `GridEngineRegistry` and two placeholder engines: `MGRSEngine` / `ISEA4HEngine`.
  - Updated `GridService` and `TopologyService` to resolve engines via registry instead of geohash-only branch logic.
  - Extended ST code prefix map to support `mgrs` and `hx`.
  - Added routing and prefix tests for new types.
- Validation:
  - `python -m pytest -q tests`
  - Result: 22 passed.
- Next:
  - Start `TASK-0007`: implement first-phase MGRS capabilities (locate + parse/geometry basics).

## 2026-03-09 | TASK-0007 | Implement first-phase MGRS capability
- Goal: Deliver first runnable MGRS engine capability in MVP (locate + basic reverse geometry).
- Scope: dependency setup, MGRS engine implementation, API/service test coverage, docs sync.
- Key Changes:
  - Added dependency `mgrs` in `requirements.txt`.
  - Implemented `MGRSEngine` first-phase methods: `locate_point`, `code_to_bbox`, `code_to_center`, `code_to_geometry`.
  - Kept unsupported methods (`cover/neighbors/parent/children`) as explicit `NotImplementedCapabilityError`.
  - Added/updated tests for MGRS engine behavior, API locate/geometry route, and service routing.
  - Updated README files to reflect multi-engine current status.
- Validation:
  - `python -m pytest -q tests`
  - Result: 28 passed.
- Next:
  - Start `TASK-0008`: ISEA4H API-level placeholder completion with consistent response contracts.

## 2026-03-09 | TASK-0008 | Complete ISEA4H API placeholder error contract
- Goal: Keep ISEA4H as phased placeholder while making API responses consistent and explicit.
- Scope: global exception mapping, error-handler tests, log/docs sync.
- Key Changes:
  - Updated global error handler to map `NotImplementedCapabilityError` to HTTP `501`.
  - Preserved unified error body contract: `{"error": {"code", "message"}}`.
  - Added tests to verify error-handler status/body mapping for `NotImplementedCapabilityError` and `ValidationError`.
- Validation:
  - `python -m pytest -q tests`
  - Result: 30 passed.
- Next:
  - Start `TASK-0009`: MGRS topology capability enhancement (neighbors/parent/children).

## 2026-03-09 | TASK-0009 | Enhance MGRS topology capability
- Goal: Move MGRS from locate/geometry-only to basic topology operations for API usability.
- Scope: MGRS engine topology methods, API tests, docs sync.
- Key Changes:
  - Implemented `MGRSEngine.neighbors` using UTM offsets at current MGRS precision level.
  - Implemented `MGRSEngine.parent` and `MGRSEngine.children` with level validation and deterministic outputs.
  - Added validation for invalid `k` and invalid/non-progressive child target levels.
  - Added/updated tests in `tests/test_mgrs_engine.py` and `tests/test_api.py` for MGRS topology methods.
  - Updated README files to reflect MGRS topology capability status.
- Validation:
  - `python -m pytest -q tests`
  - Result: passed.
- Next:
  - Start `TASK-0010`: MGRS geometry cover capability (intersect baseline).
