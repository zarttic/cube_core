# 剖分测试数据清理指南

更新时间：2026-07-13

警告：本文包含删除 OpenGauss 表数据和 MinIO 对象的命令，只能用于已确认的测试 schema/bucket。生产环境必须先备份并取得书面批准。

本文说明在重新测试剖分前，如何清理上一次剖分产生的运行数据，同时保留 ARD 载入源数据、业务配置和 MinIO 源文件。

## 清理边界

默认保留：

- OpenGauss 业务配置表，例如 `cube_web_configs`。
- 用户、订单、团队等业务表。
- ARD 载入数据表，例如 `ard_manifests`、`ard_partition_batches`、`ard_partition_assets`、`ard_partition_observations`。
- 剖分待处理队列表，例如 `partition_batches`、`partition_assets`。这些表保存从 ARD 载入数据同步来的待剖分批次和资产，默认只重置状态，不删除数据。
- MinIO 源数据目录 `s3://cube/cube/source/`，包括 `cube/source/product/`。

默认清理：

- 剖分任务运行记录：`partition_job_attempts`。
- 质检报告：`quality_reports`。
- 剖分/入库结果表：`rs_cube_cell_fact`、`rs_raw_scene_asset`、`rs_product_cell_fact`、`rs_product_asset`、`rs_entity_tile_asset`、`rs_carbon_observation_fact`、`rs_ingest_job`、`tile_metrics`。
- MinIO 中非源数据的剖分输出，例如 `cube/entity/`、`cube/perf/`、`cube/benchmark/`、`cube/grid_adapted_tests/`、`cube/smoke/`、`cube/product/`。

`cube/product/` 是剖分/入库输出目录，可以清理；`cube/source/product/` 是源数据目录，默认不能清理。

## 清理前检查

确认服务和任务没有正在写入数据。至少确认当前没有正在执行剖分任务，必要时先停止后端服务或取消任务。

```bash
ss -ltnp | rg ':50039|:50040|:50041|:50042' || true
```

确认运行时配置指向目标 OpenGauss 和 MinIO。

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
from cube_split import runtime_config

minio = runtime_config.minio_settings()
dsn = runtime_config.postgres_dsn()
print("postgres:", dsn.split("@", 1)[-1] if "@" in dsn else bool(dsn))
print("minio:", minio.endpoint, minio.bucket)
PY
```

## 只清剖分结果，重置载入批次状态

这是常用测试清理方式。它会保留 `ard_*`、`partition_batches`、`partition_assets` 和 `cube/source/*` 源数据，只把批次和资产状态改回待剖分。

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
import json
import psycopg
from cube_split import runtime_config

schema = "remote_user"
delete_tables = [
    "partition_job_attempts",
    "quality_reports",
    "rs_carbon_observation_fact",
    "rs_cube_cell_fact",
    "rs_entity_tile_asset",
    "rs_product_cell_fact",
    "rs_product_asset",
    "rs_raw_scene_asset",
    "rs_ingest_job",
    "tile_metrics",
]

report = {}
with psycopg.connect(runtime_config.require_postgres_dsn()) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema=%s AND table_type='BASE TABLE'
            """,
            (schema,),
        )
        existing = {row[0] for row in cur.fetchall()}
        for table in delete_tables:
            if table not in existing:
                report[table] = {"status": "missing"}
                continue
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."{table}"')
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            after = int(cur.fetchone()[0])
            report[table] = {"before": before, "after": after}

        if "partition_assets" in existing:
            cur.execute(
                f"""
                UPDATE "{schema}"."partition_assets"
                SET status = 'pending',
                    attempt_count = 0,
                    last_error = NULL,
                    last_run_dir = NULL,
                    partitioned_at = NULL,
                    updated_at = now()
                """
            )
            report["partition_assets_status_reset"] = {"updated": cur.rowcount}

        if "partition_batches" in existing:
            cur.execute(
                f"""
                UPDATE "{schema}"."partition_batches"
                SET status = 'pending',
                    attempt_count = 0,
                    last_task_id = NULL,
                    last_error = NULL,
                    quality_status = NULL,
                    quality_report_id = NULL,
                    quality_failure_reason = NULL,
                    partitioned_at = NULL,
                    manual_required_at = NULL,
                    ingest_status = CASE
                      WHEN data_type IN ('optical', 'product', 'radar', 'entity', 'carbon') THEN 'not_ready'
                      ELSE 'not_supported'
                    END,
                    ingest_job_id = NULL,
                    ingest_error = NULL,
                    ingested_at = NULL,
                    updated_at = now()
                """
            )
            report["partition_batches_status_reset"] = {"updated": cur.rowcount}

        if "ard_partition_batches" in existing:
            cur.execute(
                f"""
                UPDATE "{schema}"."ard_partition_batches"
                SET status = 'pending',
                    updated_at = now()
                WHERE status IS DISTINCT FROM 'pending'
                """
            )
            report["ard_partition_batches_status_reset"] = {"updated": cur.rowcount}
    conn.commit()

print(json.dumps(report, ensure_ascii=False, indent=2))
PY
```

## 清理 MinIO 剖分输出，保留 source

默认只保留 `cube/source/`，删除同 bucket 下其它 `cube/*` 输出。

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
from collections import Counter
from minio import Minio
from minio.deleteobjects import DeleteObject
from cube_split import runtime_config

minio = runtime_config.minio_settings()
client = Minio(minio.endpoint, access_key=minio.access_key, secret_key=minio.secret_key, secure=minio.secure)

keep_prefixes = ("cube/source/",)
delete_keys = []
before_count = Counter()
before_size = Counter()

for obj in client.list_objects(minio.bucket, prefix="cube/", recursive=True):
    key = obj.object_name
    top = "/".join(key.split("/")[:2])
    before_count[top] += 1
    before_size[top] += int(obj.size or 0)
    if not key.startswith(keep_prefixes):
        delete_keys.append(key)

print("before")
for prefix in sorted(before_count):
    print(prefix, before_count[prefix], before_size[prefix])

print("delete_candidates", len(delete_keys))
errors = list(client.remove_objects(minio.bucket, (DeleteObject(key) for key in delete_keys)))
print("delete_errors", len(errors))
for err in errors[:20]:
    print("ERR", err.object_name, err.code, err.message)

after_count = Counter()
after_size = Counter()
for obj in client.list_objects(minio.bucket, prefix="cube/", recursive=True):
    key = obj.object_name
    top = "/".join(key.split("/")[:2])
    after_count[top] += 1
    after_size[top] += int(obj.size or 0)

print("after")
for prefix in sorted(after_count):
    print(prefix, after_count[prefix], after_size[prefix])
PY
```

## 可选：删除非载入来源的临时批次

如果前端列表里有通过直接 `/tasks/run` 生成的临时批次，并且确定它们不是 ARD 载入数据，可以删除 `source_system='runtime'` 的批次。正常测试不需要执行本节。

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
import psycopg
from cube_split import runtime_config

with psycopg.connect(runtime_config.require_postgres_dsn()) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM remote_user.partition_job_attempts
            WHERE batch_id IN (
              SELECT batch_id
              FROM remote_user.partition_batches
              WHERE source_system = 'runtime'
            )
            """
        )
        deleted_attempts = cur.rowcount
        cur.execute(
            """
            DELETE FROM remote_user.partition_assets
            WHERE batch_id IN (
              SELECT batch_id
              FROM remote_user.partition_batches
              WHERE source_system = 'runtime'
            )
            """
        )
        deleted_assets = cur.rowcount
        cur.execute("DELETE FROM remote_user.partition_batches WHERE source_system = 'runtime'")
        deleted_batches = cur.rowcount
    conn.commit()

print("deleted runtime batches:", deleted_batches)
print("deleted runtime assets:", deleted_assets)
print("deleted runtime attempts:", deleted_attempts)
PY
```

如果源目录本身发生变化，正确做法是重新运行 ARD 导入接口，让相同 `batch_id` 的载入批次通过 upsert 更新；通常不需要删除 `ARD_SOURCE_%`。

## 可选：连载入批次一起删除

如果这次测试要把前端列表里的旧载入批次也清空，可以在默认清理之外执行本节。这个方案会删除 `partition_batches`、`partition_assets`、`ard_partition_batches`、`ard_partition_assets`、`ard_partition_observations` 中对应记录，但仍保留 `ard_manifests`、`cube_web_configs` 和 `cube/source/*`。

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
import json
import psycopg
from cube_split import runtime_config

schema = "remote_user"
delete_tables = [
    "quality_reports",
    "rs_carbon_observation_fact",
    "rs_cube_cell_fact",
    "rs_entity_tile_asset",
    "rs_product_cell_fact",
    "rs_product_asset",
    "rs_raw_scene_asset",
    "rs_ingest_job",
    "tile_metrics",
]

report = {}
with psycopg.connect(runtime_config.require_postgres_dsn()) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema=%s AND table_type='BASE TABLE'
            """,
            (schema,),
        )
        existing = {row[0] for row in cur.fetchall()}

        for table in delete_tables:
            if table not in existing:
                report[table] = {"status": "missing"}
                continue
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."{table}"')
            report[table] = {"deleted": before}

        if "partition_batches" in existing:
            cur.execute(f'SELECT count(*) FROM "{schema}"."partition_batches"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."partition_batches"')
            report["partition_batches_deleted"] = {"deleted": before}
            report["partition_assets_deleted"] = {"status": "cascade_from_partition_batches"}
            report["partition_job_attempts_deleted"] = {"status": "cascade_from_partition_batches"}

        if "ard_partition_assets" in existing:
            cur.execute(f'SELECT count(*) FROM "{schema}"."ard_partition_assets"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."ard_partition_assets"')
            report["ard_partition_assets_deleted"] = {"deleted": before}

        if "ard_partition_observations" in existing:
            cur.execute(f'SELECT count(*) FROM "{schema}"."ard_partition_observations"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."ard_partition_observations"')
            report["ard_partition_observations_deleted"] = {"deleted": before}

        if "ard_partition_batches" in existing:
            cur.execute(f'SELECT count(*) FROM "{schema}"."ard_partition_batches"')
            before = int(cur.fetchone()[0])
            cur.execute(f'DELETE FROM "{schema}"."ard_partition_batches"')
            report["ard_partition_batches_deleted"] = {"deleted": before}
    conn.commit()

print(json.dumps(report, ensure_ascii=False, indent=2))
PY
```

删除顺序保持简单：

- `partition_batches` 直接删，`partition_assets` 和 `partition_job_attempts` 通过外键级联删除。
- `ard_partition_assets`、`ard_partition_observations` 先删，再删 `ard_partition_batches`。
- `ard_manifests` 不删；后续重新导入时，批次和资产会按 `batch_id` 重新写回。

## 清理后复核

数据库复核：

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
import psycopg
from cube_split import runtime_config

tables = [
    "ard_manifests",
    "ard_partition_batches",
    "ard_partition_assets",
    "ard_partition_observations",
    "partition_batches",
    "partition_assets",
    "partition_job_attempts",
    "quality_reports",
    "rs_cube_cell_fact",
    "rs_product_cell_fact",
    "rs_carbon_observation_fact",
    "rs_entity_tile_asset",
    "rs_ingest_job",
    "cube_web_configs",
]

with psycopg.connect(runtime_config.require_postgres_dsn(), autocommit=True) as conn:
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema='remote_user' AND table_name=%s
                """,
                (table,),
            )
            if cur.fetchone() is None:
                print(table, "missing")
                continue
            cur.execute(f'SELECT count(*) FROM remote_user."{table}"')
            print(table, cur.fetchone()[0])
PY
```

MinIO 复核：

```bash
CUBE_WEB_ENV_FILE=/home/lyajun/projects/cube_project/.cube_web.env \
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
from collections import Counter
from minio import Minio
from cube_split import runtime_config

minio = runtime_config.minio_settings()
client = Minio(minio.endpoint, access_key=minio.access_key, secret_key=minio.secret_key, secure=minio.secure)
counts = Counter()
sizes = Counter()

for obj in client.list_objects(minio.bucket, prefix="cube/", recursive=True):
    prefix = "/".join(obj.object_name.split("/")[:2])
    counts[prefix] += 1
    sizes[prefix] += int(obj.size or 0)

for prefix in sorted(counts):
    print(prefix, counts[prefix], sizes[prefix])
PY
```

清理成功后的典型状态：

- `partition_job_attempts`、`quality_reports`、`rs_*`、`tile_metrics` 为 0。
- `partition_batches`、`partition_assets` 仍存在，但状态重置为 `pending`。
- `ard_*` 载入数据仍存在，`ard_partition_batches.status` 重置为 `pending`。
- MinIO 只剩 `cube/source`，除非有其它明确保留前缀。

如果执行了“连载入批次一起删除”，典型状态改为：

- `partition_batches`、`partition_assets`、`partition_job_attempts` 为 0。
- `ard_partition_batches`、`ard_partition_assets`、`ard_partition_observations` 为 0。
- `ard_manifests`、`cube_web_configs` 仍保留。

## 注意事项

- 不要删除 `cube/source/`，除非明确要重置源数据。
- 不要删除 `cube_web_configs`，运行时配置不应通过测试清理重置。
- 如果前端仍显示旧批次，先确认执行的是默认“重置状态”方案还是可选“连载入批次一起删除”方案；再刷新页面，并检查 `partition_batches` 和 `ard_partition_batches` 是否还有对应记录。
- 清理期间不要运行剖分任务，否则可能出现任务表已清但 Ray worker 继续写 MinIO 的不一致状态。
