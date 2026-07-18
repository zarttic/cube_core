# Scene Domain Installation and Operations

The production domain uses `Scene` as the shared minimum unit while keeping Dataset and load batch as
independent dimensions:

```text
LoadBatch N:M Scene N:1 Dataset
                    |
                    +-- PartitionRunScene -- PartitionRun
                    +-- IngestRunScene ---- IngestRun
```

A Dataset is the long-lived management and publication object. One load batch
may contain scenes from several Datasets, and one Dataset may receive scenes in
several load batches. `partition_run_id` is generated for each execution and is
never reused as a load batch ID.

## Schema installation

The installer creates the current Dataset, Scene, load-batch, partition-run and
ingest-run tables in the new application database. It does not copy data from
the retired database and does not drop unrelated database objects.

Preview:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/migrate_scene_domain.py
```

Execute:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/migrate_scene_domain.py --execute
```

Run preview first and review the DDL count, then execute once against the new
application database. After installation, use the loader import endpoint to
populate Dataset, Scene and LoadBatch records from current source manifests.

There is no runtime mode switch. These APIs are the only production chain.

## API chain

- `GET /v1/partition/load-batches?data_type=<type>` lists load batches that
  actually contain a Scene of the requested product type.
- `GET /v1/partition/load-batches/{id}/scenes` returns Scenes grouped by Dataset.
- `POST /v1/partition/runs` accepts Dataset-specific Scene selections and grid
  settings, then creates a distinct PartitionRun.
- `GET /v1/datasets` and its detail endpoints expose management, provenance,
  quality, publication, and current output state.
- `/v1/ingest-runs` exposes Scene-level ingest status, failed-Scene retry, and
  cancellation.

After a current quality result passes, the background ingest worker claims all
queued Scenes for the same Dataset/output version as one unit. It reads the
committed managed partition tables, checks referenced MinIO objects, and runs
the existing RS upsert primitives. A Scene becomes `completed` only after
`rs_ingest_job` and the applicable `rs_raw_scene_asset`, `rs_cube_cell_fact`,
`rs_entity_tile_asset`, `rs_product_*`, or `rs_carbon_observation_fact` rows
have been verified. Failed or stale work remains retryable. Cube geometry is
derived from `partition_grid_cells.geometry`, never from its bbox.

The frontend does not ask operators to enter a load batch or partition run ID.
It selects source load batches and Scenes, then generates a new run ID.

## Acceptance

Run the cross-package suite and frontend checks:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
cd cube_web/frontend && npm run test:unit && npm run build
```

The complete real-source gate is implemented by
`cube_web/scripts/run_real_partition_acceptance.py`. The `rs_cube_cell_fact`
geometry migration and validation are documented in
[`../../cube_split/docs/CELL_GEOM_MIGRATION.md`](../../cube_split/docs/CELL_GEOM_MIGRATION.md).

The runner consumes prepared, reviewed MinIO source objects and covers all four
product types, Geohash/MGRS/ISEA4H, multi-Dataset and multi-Scene runs, task
cancel/retry, quality Pass/Warn/Fail exports, ingest, publish and withdraw. It
fails closed when a required object, infrastructure service or assertion is
missing; it does not replace the no-route-mock browser click acceptance.
