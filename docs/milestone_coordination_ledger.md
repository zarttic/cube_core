# Milestone Coordination Ledger

This record contains gate and review evidence without credentials, DSNs, or
local data paths. The M1 integration hash is the local integration commit that
includes this record.

| milestone | status | predecessor_integration_hash | integration_hash | worker_branch | worktree_root | file_ownership | worker_checkpoint_hash | review_checkpoints | L1 | L2 | L3 | L4 | evidence_paths | owner | timestamp_utc | blockers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| M1 | PASSED | - | local M1 integration commit containing this ledger | reviewed M1 handoffs | isolated `.claude/worktrees/m1-*` | ISEA4H engine/fixtures, frozen SDK callers, Web facade, Ray core, real-AOI gate | `1a88732`, `1f2eafb`, `b1a49e2`, `bda63be`, `ea3c30a`, `4827a01`, `15ee911`, `1e7609e`, `23f4a47`, `7bdd886`, `c806115`, `537f787`, `731f397` | all four review roles recorded below | `136 passed, 2 skipped` | `415 passed, 2 skipped`; `235 passed` Web | all L3 commands exited 0 | all L4 commands exited 0 | gate and review evidence below | Codex | 2026-07-15T00:00:00Z | none |
| M2 | READY | local M1 integration commit containing this ledger | - | - | - | - | - | M1 prerequisite passed | - | - | - | - | M1 ledger row above | Codex | 2026-07-15T00:00:00Z | Start only in a new isolated M2 worktree. |
| M3 | BLOCKED | - | - | - | - | - | - | M2 prerequisite not passed | - | - | - | - | - | Codex | 2026-07-15T00:00:00Z | Requires the M2 integration hash and all M2 gates. |
| M4 | BLOCKED | - | - | - | - | - | - | M3 prerequisite not passed | - | - | - | - | - | Codex | 2026-07-15T00:00:00Z | Requires the M3 integration hash and all M3 gates. |
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
