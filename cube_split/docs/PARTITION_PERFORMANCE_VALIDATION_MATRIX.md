# Partition Performance Validation Matrix

> Historical record — retained as dated evidence only; it is not the current production contract.

Baseline: `cb618e9`
Branch: `perf/entity-coverage`
Scope: converge entity-partition coverage outside the already-optimized optical single-grid path.

Document status: historical validation matrix. It is not the current production contract; use `docs/PRODUCTION_TEST_ACCEPTANCE.md` and `cube_split/docs/WORKFLOW.md` for current behavior.

## Coverage Matrix

| Entry | Payload / config | Expected path | Validation | Status |
| --- | --- | --- | --- | --- |
| Optical run/test | `grid_type=isea4h`, default `partition_method` | `cube_web` optical runner -> `run_entity_partition` | Existing `cube_web/tests/test_app.py` dispatch tests | Verified before this change |
| Product test | `grid_type=isea4h`, `grid_level_mode=manual` | product runner -> `run_entity_partition(data_type=product)` | Existing `test_product_partition_test_runner_dispatches_isea4h_to_entity_partition` | Verified before this change |
| Radar test | `grid_type=isea4h`, `grid_level_mode=manual` | radar runner -> `run_entity_partition(data_type=radar)` | Existing `test_radar_partition_test_runner_dispatches_isea4h_to_entity_partition` | Verified before this change |
| Product run | `selected_assets=s3://...`, `grid_type=isea4h`, `partition_method=entity` | product `partition_run` -> entity runner, manifest from selected assets | `cube_web/tests/test_partition_entity_coverage.py` | Verified with stub runner |
| Radar run | `selected_assets=s3://...`, `grid_type=isea4h`, `partition_method=entity` | radar `partition_run` -> entity runner, manifest from selected assets | `cube_web/tests/test_partition_entity_coverage.py` | Verified with stub runner |
| Web payload | `/partition/product/run` with `grid_type=isea4h` and entity options | FastAPI model -> workflow payload -> runner payload | `test_partition_run_route_preserves_isea4h_entity_payload_options` | Verified with sync test store |
| `grid_level` | manual `grid_level=2/3` | forwarded unchanged to entity args | Web coverage tests | Verified |
| `target_pixels_per_hex_edge` | manual value `384` | forwarded to entity args for auto-level runs | Web coverage tests | Verified |
| `ray_parallelism` | manual value `2/3` | forwarded to entity args; entity job caps by task groups | Web coverage tests and existing entity parallelism test | Verified |
| `max_cells_per_asset` | manual value `7/9/11` | forwarded to `build_grid_tasks_driver` and enforced there | `cube_split/tests/test_entity_coverage.py` | Fixed and verified |
| Entity tile default | entity partition writes clipped GeoTIFF tiles | `_write_entity_tiles` emits GeoTIFF paths/rows | Existing `test_entity_partition_writes_one_hex_file_per_band` | Verified before this change |

## Findings

- `max_cells_per_asset` was accepted by CLI/Web payloads but ignored in `run_entity_partition`; the job always passed `0` to `build_grid_tasks_driver`, disabling the limit. This change forwards the resolved value and rejects negative direct-call values.
- Product and radar Web run entries can route to entity partition with `selected_assets` backed by `s3://` source URIs without requiring a local input directory. The test coverage verifies routing and manifest creation with stubbed runners.
- Web payloads should use the lower-case value `isea4h`. UI labels may display `ISEA4H`, but FastAPI request literals are lower-case.

## Not Yet Verified

- Live Ray + MinIO execution for product/radar entity partition was not run in this branch. The safe smoke shape is one source asset, `grid_level=1`, `ray_parallelism=2`, `max_cells_per_asset=50`, `partition_backend=ray`, and a unique `minio_prefix` such as `cube/entity/coverage/<timestamp>`.
- Full `grid_level=6` entity runs remain intentionally untested here; that is IO/CPU load testing, not coverage convergence.
- OpenGauss ingest for product/radar entity outputs was not exercised; tests stub metadata by using `metadata_backend=none`.

## Validation Commands

- `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_entity_coverage.py cube_split/tests/test_entity_partition_job.py -q`: `24 passed`
- `cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests/test_partition_entity_coverage.py -q`: `3 passed`
- `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests`: `242 passed, 2 skipped`
- `cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests`: `219 passed`
