# Partition Demo Environment

This directory holds demo-only operational notes and sample runtime configuration.
Keep production partition code in `cube_split` and `cube_web`; keep demo data,
seed batches, and cluster smoke orchestration opt-in from runtime config.

## Enable Demo Seed Batches

Production startup does not load bundled demo partition batches by default.
Enable them only in a demo environment:

```bash
CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1
```

The bundled seed batches reference local or MinIO demo assets such as optical,
product, radar, and carbon samples. They should not be auto-created in a
production database.

## Run Demo Smoke

Use the local `.cube_web.env` values for PostgreSQL, Ray, and MinIO, then run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python cube_split/scripts/run_all_partition_flows_smoke.py --mode demo
```

For a lighter non-ingest check:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python cube_split/scripts/run_all_partition_flows_smoke.py --mode test
```
