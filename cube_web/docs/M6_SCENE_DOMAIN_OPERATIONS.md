# M6 Scene Domain Migration and Operations

M6 makes `Scene` the shared minimum unit while keeping Dataset and load batch as
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

## Additive migration

The migration creates the M6 tables and lineage records, then derives Scene
ownership and run history from the existing `partition_*`, `ard_*`, and `rs_*`
tables. It does not drop, truncate, rename, or delete legacy objects or rows.

Preview:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/migrate_m6_scene_domain.py
```

Execute:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/migrate_m6_scene_domain.py --execute
```

The command is idempotent. Run it twice and require identical Dataset, Scene,
relationship, run, and lineage counts before changing the read mode. Ambiguous
historical ownership is retained as a draft Dataset with an assignment issue;
it is not silently promoted or automatically ingested.

## Runtime switch

`CUBE_WEB_M6_MODE` controls the API surface:

| Value | Behavior |
| --- | --- |
| `legacy` | Legacy production APIs only. This is the default and rollback mode. |
| `shadow` | Legacy behavior remains authoritative; M6 reads stay closed. |
| `m6-read` | M6 load batch, Dataset, and ingest GET APIs are enabled. |
| `m6-primary` | M6 Scene partition, Dataset management, and ingest APIs are enabled. |

Production promotion is `legacy -> shadow -> m6-read -> m6-primary`. Rollback
only changes the value back to `legacy` and restarts the service. M6 and legacy
tables remain intact; there is no destructive down migration.

## M6 API chain

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

The MinIO-backed all-case mock gate is documented in
[`M6_MOCK_ACCEPTANCE.md`](M6_MOCK_ACCEPTANCE.md). The `rs_cube_cell_fact`
geometry migration and validation are documented in
[`../../cube_split/docs/CELL_GEOM_MIGRATION.md`](../../cube_split/docs/CELL_GEOM_MIGRATION.md).
