# Milestone Coordination Ledger

This record contains gate and review evidence without credentials, DSNs, or
local data paths. The M1 integration hash is the local integration commit that
includes this record.

| milestone | status | predecessor_integration_hash | integration_hash | worker_branch | worktree_root | file_ownership | worker_checkpoint_hash | review_checkpoints | L1 | L2 | L3 | L4 | evidence_paths | owner | timestamp_utc | blockers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| M1 | PASSED | - | local M1 integration commit containing this ledger | reviewed M1 handoffs | isolated `.claude/worktrees/m1-*` | ISEA4H engine/fixtures, frozen SDK callers, Web facade, Ray core, real-AOI gate | `1a88732`, `1f2eafb`, `b1a49e2`, `bda63be`, `ea3c30a`, `4827a01`, `15ee911`, `1e7609e`, `23f4a47`, `7bdd886`, `c806115`, `537f787`, `731f397` | all four review roles recorded below | `136 passed, 2 skipped` | `415 passed, 2 skipped`; `235 passed` Web | all L3 commands exited 0 | all L4 commands exited 0 | gate and review evidence below | Codex | 2026-07-15T00:00:00Z | none |
| M2 | PASSED | `fe6a2cd6d9d73e136453f921b593656bc1bab326` | `8df4269fce051787546a68c2dc6aa9a6f71cef4b` | `m2-final-clean2` | isolated `.claude/worktrees/m2-final-clean2` | Partition domain, strict runner, object lifecycle, M2 gates | `8df4269` | M2 L1-L4 passed before M3 start | 65 passed | 305 passed, 8 deselected | ruff, mypy, frontend build passed | real OpenGauss, MinIO, Ray gate passed | M2 acceptance handoff | Codex | 2026-07-15T10:35:00Z | none |
| M3 | PASSED | `8df4269fce051787546a68c2dc6aa9a6f71cef4b` | this local M3 integration commit | `m3-integration` | isolated `.claude/worktrees/m3-integration` | Normalized datasets/quality/publication APIs, stream export, legacy removal, quality UI, real gate | this commit | self-review, independent review findings fixed, adversarial real gate | 15 focused tests passed | 426 passed, 1 skipped, 20 deselected for non-real split/Web regression; 290 passed, 19 deselected for Web | ruff and diff check passed; frontend build passed | 2 passed, 0 skipped: actual OpenGauss, MinIO, Ray; defect manifest drove actual Ray/worker findings; full and filtered CSV/JSON counts equal same-filter DB counts; active to withdrawn history retained | `cube_web/scripts/run_m3_quality_publication_gate.py`; `cube_web/tests/real/test_m3_quality_publication_real.py` | Codex | 2026-07-15T16:04:29Z | no publication gateway required or used |
| M1-M3 Integration | PASSED | `cf449555b9ec68dcfb620b8fd023d38ac138a204` | this acceptance commit | `m1-m3-chain-acceptance` | isolated `.claude/worktrees/m1-m3-chain-acceptance` | M1 SDK coverage, M2 strict Ray/MinIO/OpenGauss output and outbox, M3 quality/API/export/publication integration gate | this commit | focused test, non-real regression, static/build checks, compatible M3 real gate, and chained real gate passed | 6 focused quality tests plus 1 chained real test passed | non-real regression passed with real-cluster markers explicitly deselected; Web non-real regression `290 passed, 20 deselected` | ruff, mypy, SDK build, frontend production build, and diff check passed | existing M3 real gate `2 passed`; chained gate `1 passed`, both no skip/deselection; one real MinIO COG exercised all three modules; declared M3 rule generated exactly 501 errors; CSV/JSON full and filtered parsed counts and `X-Export-Count` equal same-filter OpenGauss counts; active-to-withdrawn history retained | `cube_web/scripts/run_m1_m3_chained_acceptance.py`; `cube_web/tests/real/test_m1_m3_chained_real.py`; M3 runner/test above | Codex | 2026-07-15T16:34:19Z | integration coverage only; no publication gateway implemented or invoked |
| M4 | READY | this local M3 integration commit | - | - | create new isolated worktree from the M3 commit | Consume frozen M3 normalized dataset, quality, export, and publication APIs | - | M3 prerequisite passed | - | - | - | - | M3 ledger row above | Codex | 2026-07-15T16:04:29Z | none |
| M5 | BLOCKED | - | - | - | - | - | - | M4 prerequisite not passed | - | - | - | - | - | Codex | 2026-07-15T00:00:00Z | Requires the M4 integration hash and all M4 gates. |

## M1 Gate Evidence

| level | command | exit | observed |
| --- | --- | --- | --- |
| L1 | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/test_isea4h_vectors.py cube_encoder/tests/test_isea4h_properties.py cube_encoder/tests/test_isea4h_cover.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_entity_partition_job.py cube_split/tests/test_aoi_reader.py cube_split/tests/test_carbon_ingest_query.py cube_split/tests/test_carbon_partition_job.py cube_split/tests/test_partition_services.py cube_split/tests/test_logical_partition_benchmark_script.py cube_split/tests/test_partition_e2e_smoke.py -q` | 0 | 136 passed, 2 skipped |
| L2 | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests -q` | 0 | 415 passed, 2 skipped |
| L2 | `cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests -q` | 0 | 235 passed |
| L3 | `python3.11 -m ruff check cube_encoder cube_split cube_web` | 0 | all checks passed |
| L3 | `python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web` | 0 | 111 source files, no issues |
| L3 | `cd cube_encoder && python3.11 -m build` | 0 | sdist and wheel built |
| L3 | `cd cube_web/frontend && npm ci && npm run build` | 0 | production build completed |
| L4 | `cd cube_encoder && PYTHONPATH=. python3.11 -m grid_core.app.perf_smoke` | 0 | all thresholds passed; ISEA cover averages below 3 ms |
| L4 | `CUBE_GRID_REAL_AOI_URI=<readable-real-raster> PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/integration/test_grid_real_aoi.py -v -m real_aoi` | 0 | 1 passed; no skip, deselection, or mock fallback |

## M1 Review Evidence

| role | reviewer | reviewed hashes | disposition |
| --- | --- | --- | --- |
| implementer self-review | Codex | all M1 handoffs listed in the M1 row | checked diff scope, frozen SDK call sites, and narrow tests before each handoff |
| independent reviewer | Mencius | `ea3c30a`, `ca19252`, `7bdd886` | PASS; Ray core, Web typing, and annual product ST bridge |
| independent reviewer | Wegener | `15ee911 -> 1e7609e -> 23f4a47`, `c806115`, `537f787`, `731f397` | initial findings fixed; final PASS for every listed handoff |
| independent reviewer | Ramanujan | `b1a49e2`, `23f4a47` | PASS; Web grid matrix, caller scripts, and final Web service matrix |
| adversarial verifier | Mencius | staged final integration before `731f397` | found ledger and Web time-contract P1s; both fixed and independently re-reviewed |
| final integrator | Codex | staged `m1-acceptance-final` | replayed reviewed handoffs with `cherry-pick --no-commit`, reran L1-L4, and verified no stale production SDK grid calls |
