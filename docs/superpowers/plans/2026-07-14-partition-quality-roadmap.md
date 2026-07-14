# Partition Quality Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the partition and quality refactor through five strictly ordered, independently gated milestones without allowing contract drift or unverified integration.

**Architecture:** M1 freezes the grid SDK contract; M2 builds the transactional dataset partition domain on it; M3 builds normalized dataset, quality, export, and publication services; M4 consumes those normalized APIs in the frontend; M5 only verifies cleanup and end-to-end acceptance. This umbrella roadmap owns sequencing, interfaces, evidence, and release blocking while each linked milestone plan owns executable implementation microsteps.

**Tech Stack:** Python 3.11, pytest, Ruff, mypy, FastAPI, OpenGauss/psycopg, MinIO, Ray, Vue 3, Vite, npm, Playwright, Claude Opus 4.8, Claude Sonnet 5, Claude Haiku 4.5.

## Global Constraints

- The prerequisite DAG is strictly `M1 -> M2 -> M3 -> M4 -> M5`; a milestone may start only after its predecessor is integrated and its four-level gate is recorded as passing.
- Before any kickoff, the integration base must contain all five landed plan files listed in **Required Landed Plans Before Kickoff**. A plan existing only in a worker worktree, an uncommitted file, or a path outside the integration base does not satisfy this prerequisite.
- Only `geohash`, `mgrs`, and `isea4h` remain production grid types; Geohash/MGRS are logical and ISEA4H is entity partitioning.
- The partition subsystem consumes supplied COG URIs and must not generate, convert, or reproject COGs.
- Real gates use configured OpenGauss, MinIO, and Ray with real/supplied data; mocks, SQLite, in-memory stores, synthetic substitutes, and local/thread/process substitutes cannot satisfy a real gate.
- Each milestone uses TDD and its own isolated worker worktrees created with `superpowers:using-git-worktrees`; workers never share a writable checkout or implement from the canonical checkout.
- Workers create local checkpoint commits only. The Opus integrator reviews and applies approved slices to the milestone integration worktree **without committing the integration worktree** until all four levels and review checkpoints pass; only then does it produce one local milestone integration commit, and it never pushes.
- Any failed command, missing infrastructure prerequisite, skip/block in a required real scenario, unresolved interface mismatch, or incomplete evidence blocks the current milestone and every successor.
- No implementation work may edit this roadmap to weaken a frozen interface or gate; such a change requires an explicit roadmap decision before work resumes.
- M5 is verification-only ownership: it may add acceptance/scanner/evidence assets and historical labels, but it must return production defects to M1–M4 rather than implement new product behavior.

---

## Required Landed Plans Before Kickoff

All of these exact repository-relative files must be present in the integration base and included in its reachable committed history before the M1 kickoff command may run:

1. `docs/superpowers/plans/2026-07-14-m1-grid-sdk.md`
2. `docs/superpowers/plans/2026-07-14-m2-partition-domain.md`
3. `docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md`
4. `docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md`
5. `docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md`

Run this check from the integration-base root before creating an implementation worktree:

```bash
set -euo pipefail
for plan in \
  docs/superpowers/plans/2026-07-14-m1-grid-sdk.md \
  docs/superpowers/plans/2026-07-14-m2-partition-domain.md \
  docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md \
  docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md \
  docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md
do
  test -f "$plan"
  git log -1 --format=%H -- "$plan" | grep -E '^[0-9a-f]{40}$'
done
```

Expected: five nonempty 40-character commit hashes print. Any missing file or empty hash is a kickoff blocker; do not create M1–M5 implementation worktrees.

---

## Milestone Plans and Prerequisite DAG

| Milestone | Plan | Depends on | State at kickoff |
| --- | --- | --- | --- |
| M1 — Grid SDK | `docs/superpowers/plans/2026-07-14-m1-grid-sdk.md` | none | **READY** |
| M2 — Partition Domain | `docs/superpowers/plans/2026-07-14-m2-partition-domain.md` | M1 integrated and passed | **BLOCKED** |
| M3 — Dataset Quality and Publication | `docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md` | M2 integrated and passed | **BLOCKED** |
| M4 — Frontend Architecture | `docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md` | M3 integrated and passed | **BLOCKED** |
| M5 — Cleanup and Acceptance | `docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md` | M4 integrated and passed | **BLOCKED** |

No milestone plan may be merged or executed out of this order. A successor becomes ready only when the predecessor's integration commit hash and all required evidence are recorded in the progress ledger; never substitute a hardcoded or prose-only predecessor hash.

## Frozen Cross-Milestone Interfaces

1. **M1 -> all consumers:** Every public grid request carries `requested_grid_level`; every `GridCell` carries the resolved integer `grid_level`; topology responses expose canonical `topology_code`. M2–M4 preserve these names and meanings end to end and do not infer a different level from a returned code.
2. **M2 -> M3/M4:** M2 exclusively owns the complete structural DDL and the transactional dataset/publication/cleanup stores, including dataset/current-result state, tiles/indexes/grid cells/bands/assets, quality/publication structural tables and cleanup reads, completion state/time, and an outbox event commit atomically. M2 also exclusively owns the partition-run route. Version/current pointers change only in that transaction; readers never observe a current pointer to partial details; retries are idempotent.
3. **M3 -> M4:** M3 exclusively owns business workflows and the normalized dataset, quality-record/detail/error-export, publication, and withdrawal APIs defined in `docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md`. M4 consumes these normalized APIs directly; it may add typed clients/view models but no legacy route adapter, per-data-type duplicate API, or frontend-only status semantics.
4. **Release-blocking publication lifecycle:** M2 DDL supplies the publication fields that M3 workflow/API uses; M3 defines publication `status` exactly as `publishing|active|withdrawing|failed|withdrawn`; M4 maps those values unchanged. Dataset-derived `publish_status` is exactly `unpublished|publishing|active|withdrawing|failed|withdrawn`, and uses `unpublished` only when no publication exists. The value `published` is forbidden. A mismatch across the M2 DDL -> M3 workflow/API -> M4 mapping path blocks release.
5. **M5 ownership:** M5 consumes the integrated contracts and owns only scanner/acceptance/evidence work allowed by its plan. Production failures are logged with the owning milestone, exact command, expected/observed result, and blocking status.

## Worktree, Model, Commit, and Merge Rules

- Main integration for every milestone is led and finally reviewed by **Claude Opus 4.8**. The canonical checkout is coordination-only and never hosts worker implementation edits.
- Create one isolated worktree root and branch per independently reviewable slice. Pin the assigned model for that worker's entire slice; do not switch models mid-worktree. The ledger must record each worker branch, worktree-root path, exclusive file ownership, checkpoint hash, review checkpoint, and approved/rejected state.
- **M1:** Opus owns coordination, shared standards/contracts, MGRS, and ISEA4H; Sonnet owns bounded Geohash, facade, and performance slices; Haiku owns read-only inventory only.
- **M2:** Opus owns complete structural DDL plus dataset/publication/cleanup stores and the partition-run route; Sonnet owns contracts, Ray, object handling, and gates; Haiku owns configuration cleanup.
- **M3:** Opus owns transaction, quality, and publication business workflows; Sonnet owns normalized dataset/quality APIs, export, and rules; Haiku owns legacy deletion.
- **M4:** Every worker implements only from its isolated worktree root: Sonnet owns component/view implementations; Opus owns router, interface consumption, and final integration; Haiku owns mechanical cleanup. No M4 code may be implemented in the canonical checkout.
- **M5:** Opus owns acceptance integration and final release decision; Sonnet owns residual review and browser acceptance; Haiku owns documentation/scanner inventory. The documentation/scanner worker is assigned an isolated worktree root, branch, exclusive files, and local checkpoint; the ledger records its reviewer and reviewed hash before its changes can be applied.
- Each worker runs focused tests, commits a local checkpoint, and reports its hash plus evidence. Opus records and completes the implementer, independent-review, adversarial-verification, and final-integration review checkpoints before applying an approved slice to the milestone integration worktree.
- Integrate only after rebasing or recreating the worker from the accepted predecessor integration hash in the progress ledger. Never merge a worktree carrying another milestone's unreviewed changes.
- During assembly, the milestone integration worktree has **no commit** for un-gated integration changes. Each milestone creates exactly one local integration commit only after its full L1–L4 gate and review checkpoints pass. Do not push any worker or integration branch.

## Four-Level Gate and Failure Policy

Every milestone must pass all four levels:

1. **L1 — TDD:** the milestone's narrow, task-focused tests prove the changed contract and failure paths.
2. **L2 — Regression:** the canonical encoder/split and web regression suites pass.
3. **L3 — Static/build:** Ruff, mypy, Python package builds, and the frontend build pass; M4/M5 additionally run their required unit/browser acceptance.
4. **L4 — Real:** the milestone-specific non-skipping real-data or real-infrastructure harness passes with the exact semantic assertions below.

Review roles are separate from L1–L4: (1) implementer self-review, (2) independent reviewer, (3) adversarial verifier, and (4) final Opus integrator. Each role records reviewer, reviewed worker/integration hash, finding disposition, and evidence path in the ledger before the milestone may pass.

On failure: stop integration; preserve stdout/stderr and generated JSON; record the owner, worktree commit, command, exit status, expected result, observed result, and artifact paths; fix in the owning milestone; rerun the failed level and all later levels. A required real scenario may not be marked passed by skip, xfail, environment block, deselection, or mock fallback. M5 may record ordinary pytest skips outside its required real harness, but all six M5 real scenarios must execute and pass.

## Progress and Evidence Ledger

Maintain the following row in the coordination record for every milestone; update it after each gate, not from memory at milestone end:

```text
milestone | status | predecessor_integration_hash | integration_hash | worker_branch | worktree_root | file_ownership | worker_checkpoint_hash | review_checkpoints | L1 | L2 | L3 | L4 | evidence_paths | owner | timestamp_utc | blockers
```

Allowed status values are `BLOCKED`, `READY`, `IN_PROGRESS`, `FAILED`, and `PASSED`. The `predecessor_integration_hash` is copied from the immediately preceding `PASSED` ledger row; it is the only allowed predecessor reference, and roadmap prose never hardcodes a substitute hash. Evidence must include exact commands, exit codes, test pass/fail/skip/deselected counts, real scenario counts, build result, redacted infrastructure identity, generated report/JSON paths, file ownership, worker branch/root, checkpoint hash, and each review role's reviewed hash and disposition. Never record DSNs, access keys, secret keys, tokens, or environment-file contents. Mark a successor `READY` only after the predecessor row is `PASSED` with a local integration hash.

### Task 1: M1 — Grid SDK

**Files:**
- Execute: `docs/superpowers/plans/2026-07-14-m1-grid-sdk.md`
- Evidence: milestone coordination record and M1-generated gate artifacts

- [ ] Complete every M1 plan task, preserving `requested_grid_level`, `GridCell.grid_level`, and `topology_code`, then run the canonical gate from the repository root.

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
cd .. && python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
cd cube_web/frontend && npm ci && npm run build
cd ../.. && PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/test_isea4h_vectors.py cube_encoder/tests/test_isea4h_properties.py -v
cd cube_encoder && PYTHONPATH=. python3.11 -m grid_core.app.perf_smoke
cd .. && CUBE_GRID_REAL_AOI_URI="${CUBE_GRID_REAL_AOI_URI:?set CUBE_GRID_REAL_AOI_URI to a readable real raster}" PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/integration/test_grid_real_aoi.py -v -m real_aoi
```

Expected: every command exits `0`; all fixed-vector/property tests pass without a DGGRID executable, ISEA4H normative conformance covers every resolution `r=0..15` as defined by the M1 specification and is not reduced to a low-resolution audit subset; performance smoke reports all nine averages below their thresholds; the mandatory real-AOI harness executes all collected tests and passes with `CUBE_GRID_REAL_AOI_URI` naming a readable real raster. A missing or unreadable real asset is `BLOCKED`, never skipped or passed. Record evidence, create the local M1 integration commit, mark M1 `PASSED`, and only then mark M2 `READY`.

### Task 2: M2 — Partition Domain

**Files:**
- Execute: `docs/superpowers/plans/2026-07-14-m2-partition-domain.md`
- Evidence: milestone coordination record and M2-generated gate artifacts

- [ ] After the M1 `PASSED` ledger row supplies its integration hash, complete every M2 plan task, preserving the complete structural DDL, dataset/publication/cleanup stores, partition-run route, transactional domain store, outbox, and current-pointer contract, then run the canonical gate from the repository root.

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
cd .. && python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
cd cube_web/frontend && npm ci && npm run build
cd ../.. && PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_real.py -m m2_real -vv -s
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m2_partition_domain_gate.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Expected: every command exits `0`; real OpenGauss, MinIO, and Ray are used; pytest reports exactly `6 passed`, `0 failed`, `0 skipped`, and `0 xfailed`; operator JSON reports `geohash_logical`, `isea4h_entity`, `partial_failure`, `atomic_rollback`, `unknown_commit`, and `cleanup` all `passed` with `"skipped": 0`; atomic completion/current pointers, outbox delivery/idempotency, supplied-COG consumption, and Ray execution are proven without mock or local fallback. Record evidence, create the local M2 integration commit, mark M2 `PASSED`, and only then mark M3 `READY`.

### Task 3: M3 — Dataset Quality and Publication

**Files:**
- Execute: `docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md`
- Evidence: milestone coordination record and M3-generated gate artifacts

- [ ] After the M2 `PASSED` ledger row supplies its integration hash, complete every M3 plan task and freeze the normalized APIs consumed by M4, then run L1–L3 from the repository root and use the following **only** L4 command (do not duplicate the runner's internal pytest command in this roadmap).

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
cd .. && python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
python3.11 -m build cube_encoder
python3.11 -m build cube_split
python3.11 -m build cube_web
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run build
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py
```

Expected: every command exits `0`. The non-skipping M3 runner is the sole L4 authority: it invokes the `m3_real` marker, fails on skipped or deselected M3-real tests and missing/unreachable infrastructure, and covers actual OpenGauss, MinIO, Ray, publication gateway, full/filtered CSV and JSON export counts equal to parameterized OpenGauss counts, publication snapshots, and exact-ID withdrawal. Record evidence, create the local M3 integration commit, mark M3 `PASSED`, and only then mark M4 `READY`.

### Task 4: M4 — Frontend Architecture

**Files:**
- Execute: `docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md`
- Evidence: milestone coordination record, frontend build output, and Playwright artifacts

- [ ] After the M3 `PASSED` ledger row supplies its integration hash, complete every M4 plan task using only M3 normalized APIs from isolated worker worktree roots, then run the canonical gate from the repository root.

```bash
set -euo pipefail
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
(cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests)
python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run build
npm --prefix cube_web/frontend run test:unit
npm --prefix cube_web/frontend exec playwright test tests/e2e/app.spec.js
python3.11 - <<'PY'
from pathlib import Path

roots = (Path("cube_web/frontend/src"), Path("cube_web/frontend/tests"))
forbidden = (
    "s2", "tile_matrix", "plane_grid", "QualityHistoryDrawer", "/history", "/latest",
    "/report", "生成 COG", "COG 耗时", '"published"', "'published'",
)
matches = [
    f"{path}:{line_number}:{line}"
    for root in roots
    for path in root.rglob("*")
    if path.is_file()
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
    if any(token in line for token in forbidden)
]
assert not matches, "\n".join(matches)
plan_text = Path("docs/superpowers/plans/2026-07-14-m4-frontend-architecture.md").read_text(encoding="utf-8")
assert plan_text.count(".claude" + "/worktrees") == 0, plan_text
PY
```

Expected: every command exits `0`; unit and Playwright suites pass; the Python assertion scans only frontend implementation/test files so absence-test fixtures cannot self-match, and it rejects obsolete surfaces plus `published`; the separate concatenated-string assertion proves the M4 plan contains no embedded filesystem worktree path without matching its own validation expression. Dataset and quality routes consume normalized M3 APIs; M4 maps the frozen publication statuses unchanged and derives `unpublished` only for a dataset with no publication; the three-grid controls, reset behavior, complete-error export entry, responsive layouts, and stale-detail race protection work without legacy adapters. Record evidence, create the local M4 integration commit, mark M4 `PASSED`, and only then mark M5 `READY`.

### Task 5: M5 — Cleanup and Acceptance

**Files:**
- Execute: `docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md`
- Evidence: `docs/m5_cleanup_acceptance_evidence.json` and generated `/tmp/m5_*` gate artifacts

- [ ] After the M4 `PASSED` ledger row supplies its integration hash, perform verification-only M5 work from the assigned isolated worktrees and run the final four-level gate from the repository root.

```bash
set -euo pipefail
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests cube_web/tests -q -r s | tee /tmp/m5_l1_pytest.txt
PYTHONPATH=. python3.11 scripts/m5_cleanup_acceptance_scan.py --root . --json-out /tmp/m5_cleanup_scan.json
python3.11 - <<'PY'
import json
result = json.load(open('/tmp/m5_cleanup_scan.json', encoding='utf-8'))
assert result['status'] == 'pass', result
assert result['scanned_file_count'] > 0, result
assert set(result['allowlists']).issubset({rule['name'] for rule in result['rules']}), result
PY
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run build
(cd cube_web/frontend && npx playwright test tests/e2e/m5-acceptance.spec.js --project=m5-acceptance) | tee /tmp/m5_l3_playwright.txt
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_split/scripts/run_m5_real_acceptance.py --asset-manifest /home/lyajun/projects/cube_project/acceptance_inputs/m5_six_supplied_cogs.json --run-id m5-$(date -u +%Y%m%dT%H%M%SZ) --summary-path /tmp/m5_real_acceptance_summary.json
python3.11 - <<'PY'
import json
summary = json.load(open('/tmp/m5_real_acceptance_summary.json', encoding='utf-8'))
assert summary['m5_gate_status'] == 'PASS', summary
assert summary['scenario_count'] == 6, summary
assert summary['passed_count'] == 6, summary
assert summary['failed_count'] == 0, summary
assert all(item['status'] == 'pass' for item in summary['scenarios']), summary
PY
```

Expected: every command exits `0`; ordinary pytest skips are retained in evidence; scanner count is positive with no findings; frontend build/browser acceptance pass; exactly six real OpenGauss/MinIO/Ray scenarios execute with `6` pass and `0` fail/skip/block; export/quality/publication and grid-level contracts reconcile; final evidence reports and prints `M5_GATE_STATUS=PASS`. Only then create the single local M5 integration commit and mark the roadmap `PASSED`; do not push.

## Kickoff — Start M1 Only

- [ ] Run **Required Landed Plans Before Kickoff** from the integration base and attach all five committed-path hashes to the ledger; this verifies the roadmap plus all five milestone plan files, including the exact M3 path, are landed before kickoff.
- [ ] Record the integration-base hash and set ledger states to `M1=READY`, `M2=BLOCKED`, `M3=BLOCKED`, `M4=BLOCKED`, `M5=BLOCKED`; record M1's branch, isolated worktree root, file ownership, and four review checkpoints before any implementation edit.
- [ ] Create only the isolated M1 worker worktrees and assign the M1 Opus/Sonnet/Haiku slices above; do not implement from the canonical checkout.
- [ ] Start execution of `docs/superpowers/plans/2026-07-14-m1-grid-sdk.md`.
- [ ] Do not create M2–M5 implementation worktrees, dispatch their workers, or edit their implementation files until their predecessor's `PASSED` ledger row provides the required integration hash.
