# Milestone 5 Cleanup and Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify the integrated M1–M4 product without changing its behavior, remove only stale active-tree references, and publish deterministic evidence that exactly six real OpenGauss/MinIO/Ray acceptance scenarios pass.

**Architecture:** M5 is a verification, cleanup, and evidence milestone. Owner failures are returned to M1–M4 as structured residuals; M5 adds only an inventory-aware scanner, a package-module real acceptance runner, mandatory browser acceptance, current/historical documentation maintenance, and a deterministic evidence assembler. The real runner consumes M2 strict loader-owned COG `DatasetInput` manifests and exercises M3 quality/publication behavior without adding product behavior.

**Tech Stack:** Python 3.11, pytest 8, Ruff, mypy, FastAPI, Vue 3, Element Plus, Playwright, OpenGauss through psycopg 3, MinIO Python client, Ray, rasterio, repository-owned pure-Python ISEA4H.

## Global Constraints

- Milestone order is `M1 -> M2 -> M3 -> M4 -> M5`; M5 does not edit M1–M4 product behavior, production schemas, routes, services, jobs, SDK engines, or UI components.
- A failed integrated contract becomes a reproducible `residuals.M1`, `residuals.M2`, `residuals.M3`, or `residuals.M4` record and blocks M5. Return it to its owner; do not repair it in M5.
- M5-owned implementation is limited to scanner/tool tests, the M5 acceptance package/runner/tests, the mandatory M5 Playwright spec/config, current documentation, historical labels, and deterministic evidence.
- Production grids are exactly `geohash`, `mgrs`, and `isea4h`; their native levels are `1..12`, `0..5`, and `0..15`. Geohash and MGRS are logical; ISEA4H is entity.
- ISEA4H runtime and tests must not import or invoke H3 or DGGRID. Public ISEA4H `space_code` is DGGRID v8.44 `SEQNUM` as an unpadded 1-based decimal string, and `cell_count(r) == 10 * 4**r + 2` for every resolution `r` in `0..15`.
- Requests contain only `requested_grid_level`; `grid_level` and `grid_level_mode` are forbidden request fields. Returned cells retain their actual `grid_level`; a `minimal` cover is not required to return every cell at `requested_grid_level`.
- Every acceptance dataset uses M2 `StrictPartitionRequest` and complete `DatasetInput` objects with nonempty loader-owned strict COG `assets` and dataset-level `bands`. This applies to optical, radar, product, and carbon data.
- No acceptance input may be generated or synthetic. M5 must not create, convert, reproject, upload, or repair a source COG; it may use only reviewed `s3://` loader COGs that MinIO can stat and rasterio can open through the production Ray cache path.
- Conversion fields are absent, not defaulted: `convert_asset_to_cog`, `cog_workers`, `cog_overwrite`, conversion timing, source upload, and reprojection controls must not appear with `0`, `null`, `false`, empty string, or any other value.
- Exactly six real scenarios must run and pass. Missing infrastructure, missing/invalid manifests, deselection, any skip, count mismatch, or scenario failure exits nonzero; real tests never call `pytest.skip`.
- The six scenarios are exactly: Geohash logical single dataset; MGRS cross-zone/boundary logical; low-resolution ISEA4H entity; one batch/two datasets with sibling partial failure; quality failure with complete errors and full/filtered CSV/JSON export equality; Pass/Warn policy followed by publish/withdraw reconciliation.
- Publication-record lifecycle values are exactly `publishing|active|withdrawing|failed|withdrawn`; dataset-derived publication status values are exactly `unpublished|publishing|active|withdrawing|failed|withdrawn`. The value `published` is forbidden in production, current documentation, acceptance results, scanner allowlists, and evidence.
- The frozen M3 real test is `cube_web/tests/real/test_m3_quality_publication_real.py`, marker `m3_real`, registration `m3_real: actual OpenGauss, MinIO, Ray dataset quality and publication acceptance`, and sole standard gate `cube_web/scripts/run_m3_quality_publication_gate.py`.
- The canonical M3 command is `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py`; it invokes exactly `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/real/test_m3_quality_publication_real.py -v -m m3_real -rs` and fails on skip, deselection, missing infrastructure, invalid manifests, export mismatch, or publication-gateway failure.
- Required M3 manifest variables are `CUBE_M3_REAL_INPUT_MANIFEST` and `CUBE_M3_REAL_DEFECT_MANIFEST`.
- Scanner exclusions are explicit tooling/test exclusions; production and current documentation remain in scope. `docs/superpowers/` is excluded by repository-relative prefix because plans are non-normative implementation records.
- Scanner allowlists are rule-specific and inventory-backed. An allowlisted path must be an explicit rejection test for that exact rule and must contain assertions rejecting the matched token; production paths can never be allowlisted.
- Historical reports are retained byte-for-byte except for one exact historical/non-normative label inserted after the title. Current documentation is scanned as production guidance.
- Every shell pipeline uses `set -o pipefail` before `tee`; the pipeline’s true producer exit code must be recorded.
- Evidence committed under `docs/` embeds redacted command summaries, counts, SHA-256 digests, and exit codes. References to `/tmp` files alone are not evidence.
- Worker checkpoint commits occur only in isolated worktrees. The integration owner records each reviewed concrete commit hash and applies approved slices in the declared order with `git cherry-pick --no-commit "$REVIEWED_HASH"`.
- This plan is one separate plan-only commit. M5 implementation is exactly one later integration commit. Do not push.

---

## File Structure and Ownership

| Owner/worktree | File | Responsibility |
| --- | --- | --- |
| scanner/docs worker `m5/scanner-docs` | `scripts/m5_cleanup_acceptance_scan.py` | Inventory active text files, apply rule-specific rejection allowlists, scan current docs/production, and validate historical labels. |
| scanner/docs worker `m5/scanner-docs` | `tests/test_m5_cleanup_acceptance_scan.py` | Prove scope, positive inventory, exact allowlist validation, production/current-doc scanning, and H3/ISEA dependency rejection. |
| acceptance worker `m5/real-acceptance` | `cube_split/cube_split/scripts/__init__.py` | Make acceptance runners invocable as package modules. |
| acceptance worker `m5/real-acceptance` | `cube_split/cube_split/scripts/run_m5_real_acceptance.py` | Execute and reconcile exactly six real scenarios and emit a redacted deterministic summary. |
| acceptance worker `m5/real-acceptance` | `cube_split/tests/real/test_m5_real_acceptance.py` | Marked real-cluster scenario assertions; no skip path. |
| acceptance worker `m5/real-acceptance` | `cube_split/tests/test_m5_real_acceptance_runner.py` | Unit-test inventory, manifests, redaction, scenario counting, and skip/deselection failure. |
| browser worker `m5/browser` | `cube_web/frontend/tests/e2e/m5-acceptance.spec.js` | Mandatory browser stale-response and full/filtered export request regression using frozen M4 fixtures/locators. |
| browser worker `m5/browser` | `cube_web/frontend/playwright.config.js` | Add the dedicated M5 Playwright project if not already present. |
| scanner/docs worker `m5/scanner-docs` | `docs/PRODUCTION_TEST_ACCEPTANCE.md`, `cube_split/docs/WORKFLOW.md`, `cube_encoder/docs/ARCHITECTURE.md`, `cube_encoder/docs/README.md` | Current production guidance. |
| scanner/docs worker `m5/scanner-docs` | five historical reports named in Task 6 | Exact historical bodies with one non-normative label. |
| integration owner | `docs/m5_cleanup_acceptance_evidence.json` | Embedded redacted L1–L4 results, counts, digests, exit codes, reviewed hashes, and final status. |

## Contract-Owner Routing

| Failure | Owner | M5 response |
| --- | --- | --- |
| Legacy grid/H3 runtime remains, ISEA4H level/SEQNUM/count differs, or minimal cover overwrites result levels | M1 | Record path, command, expected/observed value under `residuals.M1`; stop. |
| Strict COG `DatasetInput`, dataset isolation, Ray/MinIO/OpenGauss persistence, or request-field contract differs | M2 | Record payload/count/object evidence under `residuals.M2`; stop. |
| Dataset ownership, complete errors/exports, quality policy, publication or withdrawal reconciliation differs | M3 | Record API, IDs, counts, and gateway evidence under `residuals.M3`; stop. |
| Frozen UI locator, stale response, Element Plus selection, or download behavior differs | M4 | Record Playwright trace and exact locator failure under `residuals.M4`; stop. |

### Task 1: Freeze the verification boundary and worktree/review integration order

**Files:**
- Modify: no product file
- Create later: `docs/m5_cleanup_acceptance_evidence.json`

**Interfaces:**
- Consumes: landed, reviewed M1–M4 commits.
- Produces: clean worktrees and residual records with `owner`, `command`, `exit_code`, `path`, `expected`, `observed`, and `digest`.

- [ ] **Step 1: Confirm a clean tree and create three isolated workers**

```bash
git status --short
git worktree add ../cube-m5-scanner-docs -b m5/scanner-docs
git worktree add ../cube-m5-real-acceptance -b m5/real-acceptance
git worktree add ../cube-m5-browser -b m5/browser
```

Expected: initial status is empty; scanner/docs, real-acceptance, and browser owners have non-overlapping worktrees. No product behavior file is assigned to M5.

- [ ] **Step 2: Run integrated owner gates without editing failures**

```bash
mkdir -p artifacts/m5
set -o pipefail
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests cube_web/tests -q -rs 2>&1 | tee artifacts/m5/l1-integrated-pytest.txt
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py 2>&1 | tee artifacts/m5/l1-m3-real-gate.txt
npm --prefix cube_web/frontend run build 2>&1 | tee artifacts/m5/l1-frontend-build.txt
```

Expected: each producer exits `0`; the M3 runner collects the frozen test with `-m m3_real`, no skip/deselection, and actual OpenGauss, MinIO, Ray, export, and publication-gateway coverage. `artifacts/m5/` is uncommitted staging; Task 7 embeds redacted summaries and digests.

- [ ] **Step 3: Record failures without repairing owner code**

Use this exact shape:

```json
{"owner":"M2","command":"literal command","exit_code":1,"path":"cube_web/cube_web/services/partition_workflow.py","expected":"request contains requested_grid_level only","observed":"request serialized grid_level","digest":"sha256:64-lowercase-hex"}
```

Expected: every failed owner gate blocks M5 and is handed back. No M1–M4 product behavior edit appears in any M5 worktree.

- [ ] **Step 4: Review and integrate checkpoints in fixed order**

Order is scanner/docs, real acceptance, browser. An independent reviewer records reviewed paths, findings, rerun command, and concrete hash. Populate variables from reviewed commits, verify they are hashes, then integrate:

```bash
SCANNER_DOCS_HASH=$(git -C ../cube-m5-scanner-docs rev-parse HEAD)
REAL_ACCEPTANCE_HASH=$(git -C ../cube-m5-real-acceptance rev-parse HEAD)
BROWSER_HASH=$(git -C ../cube-m5-browser rev-parse HEAD)
printf '%s\n' "$SCANNER_DOCS_HASH" "$REAL_ACCEPTANCE_HASH" "$BROWSER_HASH" | grep -E '^[0-9a-f]{40}$'
git cherry-pick --no-commit "$SCANNER_DOCS_HASH"
git cherry-pick --no-commit "$REAL_ACCEPTANCE_HASH"
git cherry-pick --no-commit "$BROWSER_HASH"
```

Expected: exactly three concrete reviewed hashes print and evidence stores them in this order. No intermediate integration commit is created.

### Task 2: Implement the inventory-aware active-tree scanner

**Files:**
- Create: `scripts/m5_cleanup_acceptance_scan.py`
- Create: `tests/test_m5_cleanup_acceptance_scan.py`

**Interfaces:**
- Produces `scan_repository(root: Path, inventory: Inventory, rules: tuple[ScanRule, ...]) -> ScanResult` and JSON containing `status`, `scanned_file_count`, `scanned_paths`, `excluded_files`, `inventory_digest`, `rules`, `allowlists`, and `errors`.

- [ ] **Step 1: Write failing scope and allowlist tests**

Tests create a temporary inventory and assert production/current docs are scanned, `docs/superpowers/plans/old.md` is excluded, count is positive, and an allowlist succeeds only for an inventoried rejection test keyed by the exact rule name. Assert failure if the key differs, path is production, path is absent, test does not assert rejection, or token differs.

```python
assert scan["scanned_file_count"] > 0
assert "cube_web/cube_web/app.py" in scan["scanned_paths"]
assert "docs/current.md" in scan["scanned_paths"]
assert scan["allowlists"] == {"legacy_grid": ["cube_encoder/tests/test_legacy_grid_rejection.py"]}
```

- [ ] **Step 2: Write dedicated H3/ISEA runtime/dependency rejection cases**

Fixtures must fail rule `isea_h3_runtime_dependency` for:

```text
cube_encoder/grid_core/app/engines/isea4h.py: import h3
cube_encoder/pyproject.toml: dependencies = ["h3>=4"]
cube_split/requirements.txt: h3==4.1.0
cube_web/docs/current.md: ISEA4H uses H3 at runtime
```

Scanner/tool tests are exact-path exclusions from token matching: `scripts/m5_cleanup_acceptance_scan.py` and `tests/test_m5_cleanup_acceptance_scan.py`. Their exclusion remains reported. Explicit inventoried rejection tests may be allowlisted only under `isea_h3_runtime_dependency`.

- [ ] **Step 3: Prove tests fail before implementation**

```bash
PYTHONPATH=. python3.11 -m pytest tests/test_m5_cleanup_acceptance_scan.py -v
```

Expected: collection fails because the scanner module is absent.

- [ ] **Step 4: Implement exact inventory and four rules**

Use `git ls-files -co --exclude-standard -z`, normalize with `PurePosixPath`, and admit declared text suffixes. Define:

```python
EXCLUDED_PREFIXES = ("docs/superpowers/", ".git/", ".claude/", "node_modules/")
TOOL_TEST_EXCLUSIONS = frozenset({"scripts/m5_cleanup_acceptance_scan.py", "tests/test_m5_cleanup_acceptance_scan.py"})
RULE_NAMES = frozenset({"legacy_grid", "cog_conversion", "legacy_request_level", "isea_h3_runtime_dependency", "forbidden_published_status"})
```

Rules reject: (1) production/current use of removed `s2`, `tile_matrix`, `plane_grid`; (2) removed COG generation/reprojection/upload fields or responsibilities; (3) request declarations/payloads with `grid_level` or `grid_level_mode`, without rejecting result-cell `grid_level`; (4) H3 imports/calls in ISEA runtime/tests, H3 declarations in dependency/lock/package metadata, or current docs claiming H3-backed ISEA4H; (5) the exact forbidden publication status token `published` in production/current docs, while accepting only publication-record lifecycle `publishing|active|withdrawing|failed|withdrawn` and dataset-derived publication status `unpublished|publishing|active|withdrawing|failed|withdrawn`. `forbidden_published_status` has no allowlist: rejection tests use explicit scanner/tool-test exclusion rather than permitting the forbidden token. Historical paths are checked for their exact label but their dated bodies are exempt from token rules. No production/current-doc path is exempt.

- [ ] **Step 5: Run scanner tests and scan**

```bash
set -o pipefail
PYTHONPATH=. python3.11 -m pytest tests/test_m5_cleanup_acceptance_scan.py -v 2>&1 | tee artifacts/m5/l2-scanner-tests.txt
PYTHONPATH=. python3.11 scripts/m5_cleanup_acceptance_scan.py --root . --json-out artifacts/m5/l2-scanner.json 2>&1 | tee artifacts/m5/l2-scanner-stdout.txt
```

Expected: exit `0`, positive count, production/current docs scanned, only declared exclusions, inventory-backed allowlists, and five passing named rules. Commit this work together with Task 6.

### Task 3: Define and unit-test the exact six-scenario package runner

**Files:**
- Create or Modify: `cube_split/cube_split/scripts/__init__.py`
- Create: `cube_split/cube_split/scripts/run_m5_real_acceptance.py`
- Create: `cube_split/tests/test_m5_real_acceptance_runner.py`
- Create: `cube_split/tests/real/test_m5_real_acceptance.py`

**Interfaces:**
- Consumes: `CUBE_M5_REAL_INPUT_MANIFEST`, `CUBE_M5_REAL_DEFECT_MANIFEST`, `CUBE_WEB_ENV_FILE`, M2 `StrictPartitionRequest`, M3 APIs/gateway, and real OpenGauss/MinIO/Ray.
- Produces: module command `python3.11 -m cube_split.scripts.run_m5_real_acceptance --summary-path artifacts/m5/l4-real-summary.json` and exactly six non-skipped scenario records.

- [ ] **Step 1: Freeze immutable scenario IDs in a failing test**

```python
assert [case.id for case in REAL_ACCEPTANCE_CASES] == [
    "geohash_logical_single_dataset",
    "mgrs_cross_zone_boundary_logical",
    "isea4h_low_resolution_entity",
    "batch_two_datasets_sibling_partial_failure",
    "quality_fail_complete_exports",
    "pass_warn_publish_withdraw_reconciliation",
]
assert len(REAL_ACCEPTANCE_CASES) == 6
```

- [ ] **Step 2: Freeze strict manifest validation**

Every dataset validates as M2 `DatasetInput`: nonempty identity/data type, loader-owned strict COG `assets`, and dataset-level `bands`; every band references an asset. Each asset has `s3://` COG URI, stable source asset ID, time, positive resolution, source CRS/transform/shape, and required footprint. Carbon is also a strict COG `DatasetInput` with carbon variables in dataset-level `bands`; no observation-only alias is accepted. Reject local paths, generated/synthetic flags, missing MinIO objects, asset-level bands, legacy aliases, conversion controls, credentials, missing case IDs, or extra case IDs.

- [ ] **Step 3: Freeze request/result-level contracts**

Each request contains `batch_id`, `grid_type`, `requested_grid_level`, `cover_mode`, `time_granularity`, `max_cells_per_asset`, and `datasets`. Assert serialized request contains neither `grid_level` nor `grid_level_mode`. Assert returned cells retain actual `grid_level`; for `minimal`, verify valid result levels without requiring equality to the requested level.

- [ ] **Step 4: Freeze pure-Python ISEA4H assertions**

For result resolution `r`, assert `0 <= r <= 15`, `space_code.isascii()`, `space_code.isdecimal()`, no leading zero, and `1 <= int(space_code) <= 10 * 4**r + 2`. Assert runtime/import graph and production dependency metadata contain no H3/DGGRID runtime dependency. Do not call H3 average-edge APIs or implement an H3/native `1..12` selector.

- [ ] **Step 5: Prove runner tests fail, then implement fail-closed collection/redaction**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_m5_real_acceptance_runner.py -v
```

Expected before implementation: module import failure. Implement the runner to launch exactly `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/real/test_m5_real_acceptance.py -v -m m5_real -rs`. Register `m5_real: actual OpenGauss, MinIO, Ray M5 cleanup acceptance`. Exit nonzero for collection count other than six, deselection, skip, invalid manifest/infrastructure, or failure. Redact credentials and absolute paths; write sorted UTF-8 JSON atomically in immutable scenario order.

### Task 4: Prove exactly six real scenarios

**Files:**
- Modify: `cube_split/tests/real/test_m5_real_acceptance.py`

**Interfaces:**
- Produces per scenario: IDs, requested/result-level summary, Ray job ID, redacted MinIO prefix, OpenGauss counts, quality/publication IDs, assertion count, elapsed milliseconds, status, and input/output digest.

- [ ] **Step 1: Geohash logical single dataset**

Submit one optical strict COG dataset with Geohash level 7. Assert one completed dataset/version, positive logical tile/index/grid counts, loader COG references, no entity object, and exact OpenGauss pointer/count reconciliation.

- [ ] **Step 2: MGRS cross-zone/boundary logical**

Submit the reviewed strict COG whose footprint crosses the frozen UTM/UPS or adjacent-zone boundary at reviewed precision `0..5`. Assert both expected domains/zones, standard `space_code`, `mgrs-topo-v1:` identity, valid clipped windows, stattable source references, and exact OpenGauss counts.

- [ ] **Step 3: Low-resolution ISEA4H entity**

Submit the reviewed product strict COG at `requested_grid_level=2`. Assert immutable versioned entity tiles, stattable MinIO objects, exact OpenGauss counts, decimal DGGRID SEQNUM bounds from each result cell’s level, and no H3/DGGRID runtime import.

- [ ] **Step 4: One batch/two datasets with sibling partial failure**

Submit one valid optical dataset and one radar defect-manifest dataset in one batch. The reviewed real defect COG passes manifest/preflight but fails deterministically during dataset-owned execution. Assert valid sibling completion/current pointer; failed sibling terminal attempt with no output/current-pointer mutation; batch counts one completed/one failed; no cross-dataset rollback/object leakage.

- [ ] **Step 5: Quality failure, complete errors, and full/filtered CSV/JSON equality**

Use the carbon strict COG from the defect manifest and require quality `fail`. Exhaustively page persisted errors into tuple multiset `(error_id, rule_code, severity, object_identity, message)`. Parse full CSV/JSON and one explicit rule/severity-filtered CSV/JSON. Assert full CSV == full JSON == all persisted errors; filtered CSV == filtered JSON == applying the predicate to full errors; response counts equal parsed counts; exports contain no pagination; at least one error lies outside the filter.

- [ ] **Step 6: Pass/Warn policy, publish, and withdraw reconciliation**

Use two completed strict COG outputs producing Pass and Warn. Assert Pass publishes through the actual gateway. Assert Warn blocks before run-specific approval, then approves and publishes. Reconcile immutable publication ID, exact dataset/output/quality snapshot, gateway state, and OpenGauss active state. For every observed transition, assert publication-record lifecycle belongs to the exact set `publishing|active|withdrawing|failed|withdrawn` and dataset-derived publication status belongs to the exact set `unpublished|publishing|active|withdrawing|failed|withdrawn`; assert `published` never appears. Withdraw both; assert gateway withdrawal, cleared active state, retained immutable history, terminal publication record `withdrawn`, dataset-derived status `withdrawn`, and idempotent reconciliation.

- [ ] **Step 7: Run module gate and checkpoint**

```bash
set -o pipefail
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_m5_real_acceptance_runner.py -v 2>&1 | tee artifacts/m5/l4-runner-unit.txt
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
CUBE_M5_REAL_INPUT_MANIFEST=/absolute/reviewed/m5-input-manifest.json \
CUBE_M5_REAL_DEFECT_MANIFEST=/absolute/reviewed/m5-defect-manifest.json \
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 -m cube_split.scripts.run_m5_real_acceptance --summary-path artifacts/m5/l4-real-summary.json 2>&1 | tee artifacts/m5/l4-real-stdout.txt
git add cube_split/cube_split/scripts/__init__.py cube_split/cube_split/scripts/run_m5_real_acceptance.py \
  cube_split/tests/test_m5_real_acceptance_runner.py cube_split/tests/real/test_m5_real_acceptance.py
git diff --cached --check
git commit -m "test: add real cleanup acceptance runner"
git log -1 --format=%H
```

Expected: tests and real run exit `0`; counts are `6/6/0/0/0` for scenario/pass/fail/skip/deselect; independent review covers all six semantics, manifests, no-skip enforcement, redaction, and module invocation and records the concrete hash.

### Task 5: Add mandatory browser race and export acceptance

**Files:**
- Create: `cube_web/frontend/tests/e2e/m5-acceptance.spec.js`
- Modify: `cube_web/frontend/playwright.config.js`

**Interfaces:**
- Consumes: frozen M4 spec `cube_web/frontend/tests/e2e/app.spec.js`; fixture module `cube_web/frontend/tests/e2e/fixtures.js`; `installApiRoutes(page, { deferQualityRunA })`; named fixtures `datasetListFixture`, `datasetDetailFixtureA`, `datasetDetailFixtureB`, `datasetAssetsFixtureA`, `datasetGridFixtureA`, `qualityRecordsFixture`, `qualityDetailFixtureA`, `qualityDetailFixtureB`, `qualityResultsFixtureA`, `qualityErrorsFixtureA`, `qualityExportCsvFixture`, and `qualityExportJsonFixture`; and returned `releaseQualityRunA()`.
- Produces: mandatory HTTP-interception stale-quality proof and full/filtered CSV/JSON export request proof. It is never replaced by a vaguely similar older test.

- [ ] **Step 1: Import and assert the literal M4 handoff**

Import `installApiRoutes` and every named fixture above from `./fixtures.js`. Use dataset identities `dataset-a`/`Dataset A` and `dataset-b`/`Dataset B`; quality identities `quality-run-a` and `quality-run-b`; exact dataset routes `**/v1/partition/datasets/dataset-a` and `**/v1/partition/datasets/dataset-b`; exact quality routes `**/v1/quality/records/quality-run-a` and `**/v1/quality/records/quality-run-b`; export API base exactly `/v1/quality/records/quality-run-a/errors/export`; and Playwright export interception glob exactly `**/v1/quality/records/quality-run-a/errors/export*`. The frozen export filenames are exactly `dataset-a_quality-run-a_errors.csv` and `dataset-a_quality-run-a_errors.json`; MIME types are exactly `text/csv` and `application/json`, respectively. If the integrated M4 commit differs, record `residuals.M4` and stop; do not adapt.

Use only these exact test IDs: `partition-grid-type`, `dataset-row-dataset-a`, `dataset-row-dataset-b`, `dataset-detail-drawer`, `dataset-detail-close`, `dataset-detail-tab-assets`, `dataset-detail-tab-grid`, `quality-row-quality-run-a`, `quality-row-quality-run-b`, `quality-detail-drawer`, `quality-detail-close`, `quality-detail-tab-results`, `quality-detail-tab-errors`, `quality-export-all`, and `quality-export-filtered`. Do not use conditional lookup, `.or()`, fallback CSS/text, index-based row guesses, adaptive route globs, or substitute IDs.

- [ ] **Step 2: Write the mandatory delayed quality-response race**

Use the frozen fixture delay rather than inventing a delay route:

```javascript
const { releaseQualityRunA } = await installApiRoutes(page, { deferQualityRunA: true });
await page.goto('/quality');
await page.getByTestId('quality-row-quality-run-a').click();
await page.getByTestId('quality-row-quality-run-b').click();
await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
await releaseQualityRunA();
await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
await expect(page.getByTestId('quality-detail-drawer')).not.toContainText('quality-run-a');
```

`installApiRoutes` implements the delay through Playwright `page.route()` interception and retains A's route until `releaseQualityRunA()` fulfills it. Do not dispatch/listen for `CustomEvent`, sleep, or replace the frozen mechanism.

- [ ] **Step 3: Exercise frozen Element Plus and export interfaces**

Open `quality-run-a`, switch with `quality-detail-tab-errors`, and select the frozen error rule through its `.el-select` trigger, `.el-select-dropdown:visible`, and visible `.el-select-dropdown__item`; native `selectOption` is forbidden. Click `quality-export-all` and assert URL has `format=csv` but no `rule_code`, `page`, or `page_size`; click `quality-export-filtered` and assert `rule_code` is present but pagination is absent. Repeat through the frozen JSON format control/fixture and assert the frozen filenames/content types. Use `quality-detail-tab-results` to prove its named result fixture renders. Also use `partition-grid-type` with visible teleported items exactly `Geohash`, `扩展 MGRS`, `ISEA4H`.

- [ ] **Step 4: Run and checkpoint browser gate**

```bash
set -o pipefail
npm --prefix cube_web/frontend ci 2>&1 | tee artifacts/m5/l3-npm-ci.txt
npm --prefix cube_web/frontend run build 2>&1 | tee artifacts/m5/l3-build.txt
npm --prefix cube_web/frontend exec playwright test tests/e2e/m5-acceptance.spec.js --project=m5-acceptance 2>&1 | tee artifacts/m5/l3-playwright.txt
git add cube_web/frontend/tests/e2e/m5-acceptance.spec.js cube_web/frontend/playwright.config.js
git diff --cached --check
git commit -m "test: add cleanup browser acceptance"
git log -1 --format=%H
```

Expected: all exit `0`, no skip; independent reviewer compares every fixture/locator literal to the reviewed M4 commit and records the concrete browser hash.

### Task 6: Update current documentation and label historical records in the scanner/docs worktree

**Files:**
- Modify: `docs/PRODUCTION_TEST_ACCEPTANCE.md`
- Modify: `cube_split/docs/WORKFLOW.md`
- Modify: `cube_encoder/docs/ARCHITECTURE.md`
- Modify: `cube_encoder/docs/README.md`
- Modify label only: `cube_split/docs/ENTITY_PARTITION_PERFORMANCE.md`
- Modify label only: `cube_split/docs/LOGICAL_PARTITION_PERFORMANCE.md`
- Modify label only: `cube_split/docs/PARTITION_OPTIMIZATION_REAL_DATA_TEST_REPORT.md`
- Modify label only: `cube_split/docs/PARTITION_PERFORMANCE_VALIDATION_MATRIX.md`
- Modify label only: `cube_web/docs/PARTITION_GRID_METHOD_AND_HISTORY.md`

**Interfaces:**
- Consumes: integrated M1–M4 contracts and Task 2 scanner.
- Produces: current normative guidance plus byte-preserved historical bodies, owned/reviewed with scanner under `m5/scanner-docs`.

- [ ] **Step 1: Update only current contract statements**

Use exactly:

```markdown
Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`.
```

State requests use `requested_grid_level`, cells retain actual `grid_level`, minimal cover may differ, strict loader COG assets/dataset-level bands are consumed unchanged, and partition never creates/reprojects source COGs. State ISEA4H uses unpadded decimal DGGRID SEQNUM with `cell_count(r) = 10 * 4**r + 2` and no H3/DGGRID runtime dependency. State publication-record lifecycle values are exactly `publishing|active|withdrawing|failed|withdrawn`, dataset-derived publication status values are exactly `unpublished|publishing|active|withdrawing|failed|withdrawn`, and `published` is forbidden.

- [ ] **Step 2: Label only and preserve historical bodies**

After each historical title insert exactly:

```markdown
> Historical record — retained as dated evidence only; it is not the current production contract.
```

Verify each body SHA-256 after removing this one label equals its pre-edit body SHA-256. Do not normalize dated content.

- [ ] **Step 3: Run gates, checkpoint, and hand off reviewed hash**

```bash
set -o pipefail
PYTHONPATH=. python3.11 -m pytest tests/test_m5_cleanup_acceptance_scan.py -v 2>&1 | tee artifacts/m5/l2-scanner-docs-tests.txt
PYTHONPATH=. python3.11 scripts/m5_cleanup_acceptance_scan.py --root . --json-out artifacts/m5/l2-scanner-docs.json 2>&1 | tee artifacts/m5/l2-scanner-docs-stdout.txt
git add scripts/m5_cleanup_acceptance_scan.py tests/test_m5_cleanup_acceptance_scan.py \
  docs/PRODUCTION_TEST_ACCEPTANCE.md cube_split/docs/WORKFLOW.md cube_encoder/docs/ARCHITECTURE.md cube_encoder/docs/README.md \
  cube_split/docs/ENTITY_PARTITION_PERFORMANCE.md cube_split/docs/LOGICAL_PARTITION_PERFORMANCE.md \
  cube_split/docs/PARTITION_OPTIMIZATION_REAL_DATA_TEST_REPORT.md cube_split/docs/PARTITION_PERFORMANCE_VALIDATION_MATRIX.md \
  cube_web/docs/PARTITION_GRID_METHOD_AND_HISTORY.md
git diff --cached --check
git commit -m "docs: clean current partition guidance"
git log -1 --format=%H
```

Expected: scanner exits `0`, current docs pass five rules, historical body digests match, and no product behavior file is staged. An independent reviewer checks inventory/exclusions/allowlists, dedicated H3 rule, forbidden `published` rule and exact lifecycle sets, current statements, and body digests; accepted fixes rerun gates and produce the concrete reviewed scanner/docs hash integrated first in Task 1.

### Task 7: Execute L1–L4 and assemble deterministic embedded evidence

**Files:**
- Create: `docs/m5_cleanup_acceptance_evidence.json`
- Modify: no product behavior file

**Interfaces:**
- Consumes: command artifacts, reviewed hashes, source commit, and residuals.
- Produces: self-contained redacted JSON with top-level `M5_GATE_STATUS`.

- [ ] **Step 1: Run all final gates with pipe failure preserved**

```bash
set -o pipefail
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests cube_web/tests -q -rs 2>&1 | tee artifacts/m5/final-l1-pytest.txt
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py 2>&1 | tee artifacts/m5/final-l1-m3-real.txt
PYTHONPATH=. python3.11 -m pytest tests/test_m5_cleanup_acceptance_scan.py -v 2>&1 | tee artifacts/m5/final-l2-tests.txt
PYTHONPATH=. python3.11 scripts/m5_cleanup_acceptance_scan.py --root . --json-out artifacts/m5/final-l2-scan.json 2>&1 | tee artifacts/m5/final-l2-scan.txt
npm --prefix cube_web/frontend ci 2>&1 | tee artifacts/m5/final-l3-ci.txt
npm --prefix cube_web/frontend run build 2>&1 | tee artifacts/m5/final-l3-build.txt
npm --prefix cube_web/frontend exec playwright test tests/e2e/m5-acceptance.spec.js --project=m5-acceptance 2>&1 | tee artifacts/m5/final-l3-browser.txt
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
CUBE_M5_REAL_INPUT_MANIFEST=/absolute/reviewed/m5-input-manifest.json \
CUBE_M5_REAL_DEFECT_MANIFEST=/absolute/reviewed/m5-defect-manifest.json \
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 -m cube_split.scripts.run_m5_real_acceptance --summary-path artifacts/m5/final-l4-summary.json 2>&1 | tee artifacts/m5/final-l4-stdout.txt
```

Expected: every producer exits `0`; M3 and M5 real gates have no skip/deselection; M5 scenario counts are `6/6/0/0/0`.

- [ ] **Step 2: Assemble embedded evidence atomically**

The assembly program reads every artifact, redacts secrets/absolute paths, computes raw SHA-256, and writes sorted JSON atomically. It embeds: source commit; integration order with three concrete reviewed hashes; per-command exit code; parsed pass/fail/skip/deselect counts; bounded redacted stdout summaries; scanner count/rule counts/inventory digest; all six redacted scenario summaries/counts/digests; publication-record lifecycle exact set `publishing|active|withdrawing|failed|withdrawn`; dataset-derived publication status exact set `unpublished|publishing|active|withdrawing|failed|withdrawn`; explicit assertion/count evidence that forbidden `published` occurrences equal zero; residuals; and redaction policy. It must not use artifact pathnames as substitutes for content.

Required assertions include:

```python
assert report["M5_GATE_STATUS"] == "PASS"
assert [x["owner"] for x in report["integration_order"]] == ["m5/scanner-docs", "m5/real-acceptance", "m5/browser"]
assert all(re.fullmatch(r"[0-9a-f]{40}", x["reviewed_commit"]) for x in report["integration_order"])
assert report["gates"]["L4"]["counts"] == {"scenario": 6, "passed": 6, "failed": 0, "skipped": 0, "deselected": 0}
assert report["publication_status_contract"] == {
    "publication_record": ["publishing", "active", "withdrawing", "failed", "withdrawn"],
    "dataset_derived": ["unpublished", "publishing", "active", "withdrawing", "failed", "withdrawn"],
    "forbidden_published_count": 0,
}
assert all(gate["summary"] and gate["sha256"] and gate["exit_codes"] for gate in report["gates"].values())
assert all(not report["residuals"][owner] for owner in ("M1", "M2", "M3", "M4"))
```

Validate committed JSON contains no `/tmp/`, `postgresql://`, credentials, absolute manifest path, or local run directory. Print exactly `M5_GATE_STATUS=PASS`; otherwise write `FAIL`, retain redacted residual evidence, and block commit.

- [ ] **Step 3: Self-review and create one M5 implementation commit**

Self-review checks: no M1–M4 product behavior edit; six exact scenarios; strict COG assets/bands including carbon; request-field absence; ISEA `0..15` decimal SEQNUM/counts; scanner scope/allowlists/H3 rule; exact M3 gate; literal frozen M4 browser handoff; pipefail; embedded evidence; reviewed hash order.

```bash
git status --short
git diff --cached --name-only
git diff --cached --check
git commit -m "test: complete cleanup acceptance milestone"
git status --short
```

Expected: staged names are only M5-owned files listed here; exactly one implementation commit; clean tree; no push.

## Plan Commit (separate from implementation)

```bash
git status --short
git add docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md
git diff --cached --name-only
git diff --cached --check
git commit -m "docs: revise cleanup acceptance milestone plan"
git status --short
```

Expected: only `docs/superpowers/plans/2026-07-14-m5-cleanup-acceptance.md` is committed; no implementation file is staged; tree is clean; do not push.
