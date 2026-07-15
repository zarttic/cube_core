# M2 Partition Domain Gate

The real gate requires an isolated destructive OpenGauss database and real
MinIO and Ray services. It never substitutes fakes or skips missing services.

Required environment variables: `CUBE_WEB_POSTGRES_DSN`,
`CUBE_WEB_M2_DATABASE_NAME`, `CUBE_WEB_MINIO_ENDPOINT`,
`CUBE_WEB_MINIO_ACCESS_KEY`, `CUBE_WEB_MINIO_SECRET_KEY`,
`CUBE_WEB_MINIO_BUCKET`, `RAY_ADDRESS`, `CUBE_WEB_M2_GEOHASH_COG_URI`, and
`CUBE_WEB_M2_ISEA4H_COG_URI`.

Preview the reset before execution:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/reset_partition_domain.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Execute the real gate only for the isolated database:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m2_partition_domain_gate.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

The probe object is confined to `partition/_m2_gate_probe/`; entity output uses
`partition/<dataset_id>/versions/<output_version>/tiles/`. The gate runs eight
non-skipping pytest cases: marker ownership plus logical, entity, partial-failure,
rollback, idempotent recovery, cleanup, and catalog-handoff scenarios. A passing
operator report marks all seven business scenarios `passed` with `skipped: 0`.
No credentials belong in this document.
