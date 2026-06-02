# Repository Guidelines

## Project Structure & Module Organization

Python monorepo at the repository root.

- `cube_encoder/` contains the core grid SDK and API models in `grid_core/`, with tests in `tests/`.
- `cube_split/` contains partitioning, Ray ingest, AOI reading, and jobs in `cube_split/`, with tests in `tests/`.
- `cube_web/` contains the FastAPI host in `cube_web/app.py`, static assets in `cube_web/web/`, and tests in `tests/`.
- Docs live in each package’s `docs/` directory where present.

`cube_encoder` is the SDK provider. Other packages must consume encoder capability through `grid_core.sdk.CubeEncoderSDK` or the web SDK backend, not duplicate grid logic.

## Build, Test, and Development Commands

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```

Runs the default encoder and split package tests. For web changes, also run:

```bash
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
```

```bash
cd cube_encoder && python -m build
```

Builds the `cube-encoder` distribution.

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.8 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

Runs the web UI with Python 3.8, the in-repo SDK, and partition backends. Runtime service endpoints are auto-loaded from local `.cube_web.env` when matching environment variables are not already set.

## Coding Style & Naming Conventions

Use Python 3.11+, 4-space indentation, type hints for public functions, and focused modules. Package names are lowercase with underscores, for example `grid_core`, `cube_split`, and `cube_web`. Tests use `test_*.py` files and descriptive `test_*` functions. Keep frontend code in plain HTML/CSS/JS.

## Execution Rules

- Prefer the smallest effective change; avoid unrelated refactors.
- Do not adjust public interfaces across packages unless the task explicitly requires it.
- When changing API behavior, check the `cube_web` call chain and update tests together.
- Before adding dependencies, confirm an existing dependency cannot solve the need.
- Do not casually move directories or rename public modules.

## Production vs Demo Separation

Keep `master`/`main` as the production development baseline. Production code owns the reusable partition execution path, managed batch workflow, retry/cancel/quality behavior, runtime configuration, and tests.

- Use `run` as the production partition operation and API name. Existing `demo` endpoints may remain only as backwards-compatible aliases for older clients; do not add new production call sites that submit `demo` operations.
- Bundled demo partition batches must be runtime opt-in. Production startup must not seed demo batches unless `CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1` is explicitly set.
- Keep demo-only docs, local `.cube_web.env` examples, seed-data manifests, smoke orchestration, and presentation scripts on `demo/*` branches, for example `demo/partition-chain-202606`.
- Do not merge demo data, local absolute data paths, credentials, or demo-specific hard-coded source manifests back into the production branch.
- General bug fixes and reusable partition capability flow from production branches into demo branches. Demo-only adjustments stay on the demo branch. If a demo run exposes a real production bug, extract the smallest fix and cherry-pick or PR it back to production.

## Testing Guidelines

The project uses `pytest`. Add or update tests beside the package being changed. For SDK/API changes, cover service behavior and FastAPI endpoints where applicable. Before pushing, run the full cross-package pytest command above; for narrow web changes, also run:

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

from `cube_web/`.

## Commit & Pull Request Guidelines

Recent history uses short imperative messages, sometimes with prefixes such as `feat:`, `docs:`, or `feat(partition):`. Keep commits focused and user-visible, for example `Update cube web SDK backend and UI`.

Use `gh` CLI for GitHub publishing. Before every push, run the full cross-package pytest command and include the result in the PR or handoff. PRs should include a summary, affected paths, validation results, UI screenshots, and related issues or notes when available.

If `gh` authentication or GitHub CLI access fails inside the sandbox, retry the same `gh` command outside the sandbox with escalation before concluding that authentication is invalid.

## Security & Configuration Tips

Do not commit local data, caches, `.pytest_cache/`, `__pycache__/`, virtual environments, or large ingest inputs. Keep service endpoints configurable; avoid hard-coding machine-specific IPs.

## Web Runtime Configuration

Web startup configuration is runtime-only. Do not store PostgreSQL DSNs, Ray addresses, MinIO endpoints, portal URLs, or credentials in the `cube_web_configs` table. That table is for user-editable business defaults only:

- `partition`
- `ingest`
- `quality`

`cube_split.runtime_config` resolves runtime values in this order:

1. Process environment variables.
2. `CUBE_WEB_ENV_FILE`, when set.
3. Local `.cube_web.env` in the current working directory or repository root.
4. Code defaults where present.

The repository ignores `.cube_web.env`. Keep it local and do not commit credentials. A local deployment file should contain at least:

```bash
CUBE_WEB_POSTGRES_DSN=postgresql://<user>:<password>@127.0.0.1:55432/cube
CUBE_WEB_RAY_ADDRESS=ray://10.136.1.13:10001
CUBE_WEB_MINIO_ENDPOINT=10.136.1.14:9000
CUBE_WEB_MINIO_BUCKET=cube
```

Current runtime endpoints:

- **PostgreSQL**: Podman container `cube-pg`, host port `127.0.0.1:55432`, database `cube`.
- **Ray**: `ray://10.136.1.13:10001`.
- **MinIO**: `10.136.1.14:9000`, bucket `cube`, `secure=false`.

The configuration page must display runtime startup information for PostgreSQL, Ray, and MinIO, but must not persist those values back into `cube_web_configs`.

Demo partition seed batches are not production configuration. Only demo environments should set:

```bash
CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1
```

Portal navigation is runtime configuration, not configuration-management data. Defaults are:

- 首页: `http://10.136.1.14:5176/#/home`
- 剖分数据服务: `http://10.136.1.14:5176/#/partition`
- 资源调度: `http://10.136.1.14:5176/#/dispatch`
- ARD数据载入: `http://10.136.1.14:5177/ard`
- 后台管理: `http://10.136.1.14:5177/admin`

---

## 基础设施集群信息

### MinIO 分布式集群

4 节点分布式集群，EC:2 纠删码。

| 节点 | IP | 主机名 | MinIO API | Console | 认证 |
|------|-----|--------|-----------|---------|------|
| .13 | 10.136.1.13 | slave01 | :9000 | :9001 | 使用本地密钥管理 |
| .14 | 10.136.1.14 | slave02 | :19010 | :19011 | 使用本地密钥管理 |
| .15 | 10.136.1.15 | slave03 | :9000 | :9001 | 使用本地密钥管理 |
| .20 | 10.136.1.20 | inspur-NF5280M4 | :9000 | :9001 | 使用本地密钥管理 |

- **Console 入口**: `http://10.136.1.14:9001`（Nginx LB 在 .14）
- **Nginx LB**: .14 上，API 端口 9000，Console 端口 9001
- **环境变量**: 各节点 `/etc/default/minio` 已配置 `MINIO_PROMETHEUS_AUTH_TYPE=public`
- **数据盘**: .13/.14/.15 为 39T/39T/31T，.20 为 2.6T（/data1）
- **SSH 认证**: 使用本地密钥管理或运维侧凭据，不在仓库记录口令。
- **认证来源**: 运行任务时优先从环境变量或节点 `/etc/default/minio` 读取 MinIO 凭据，不在仓库记录明文口令。不要继续假设 `minioadmin/minioadmin` 可用。
- **演示源数据前缀**:
  - 光学/实体剖分源影像: `s3://cube/cube/source/optocal/...`
  - 信息产品源影像: `s3://cube/cube/source/product/...`
  - 前端 demo schema 的 `source_uri` 应使用上述 `s3://` URL，不要回退为某一台机器的本地绝对路径。
- **源数据同步命令参考**:
  ```bash
  PYTHONPATH=cube_encoder:cube_split:cube_web python3.8 - <<'PY'
  from concurrent.futures import ThreadPoolExecutor, as_completed
  from pathlib import Path
  from minio import Minio
  from minio.error import S3Error

  root = Path("/home/lyjdev/projects/cube_project")
  jobs = [
      (root / "cube_split/data/product", "cube/source/product"),
      (root / "cube_split/data/optocal", "cube/source/optocal"),
  ]
  client = Minio("10.136.1.14:9000", access_key="...", secret_key="...", secure=False)
  bucket = "cube"
  if not client.bucket_exists(bucket):
      client.make_bucket(bucket)

  items = []
  for base, prefix in jobs:
      for path in sorted(p for p in base.rglob("*") if p.is_file()):
          items.append((path, f"{prefix}/{path.relative_to(base).as_posix()}"))

  def upload_one(item):
      path, key = item
      try:
          stat = client.stat_object(bucket, key)
          if stat.size == path.stat().st_size:
              return "skip", key
      except S3Error as exc:
          if exc.code not in {"NoSuchKey", "NoSuchObject"}:
              raise
      client.fput_object(bucket, key, str(path))
      return "put", key

  with ThreadPoolExecutor(max_workers=4) as pool:
      for status, key in (future.result() for future in as_completed([pool.submit(upload_one, item) for item in items])):
          print(status, f"s3://{bucket}/{key}")
  PY
  ```

### 监控栈（Prometheus + Grafana）

| 组件 | 节点 | 端口 | 访问地址 |
|------|------|------|----------|
| Prometheus | .13 | 9090 | `http://10.136.1.13:9090` |
| Grafana | .13 (Docker) | 3000 | `http://10.136.1.13:3000` |
| Node Exporter | .13/.15/.20 | 9100 | - |
| Node Exporter | .14 | 19100 | - |

- **Grafana Dashboard**:
  - MinIO Cluster: `http://10.136.1.13:3000/d/minio-cluster-v2/minio-cluster`（自定义，10 面板，工作正常）
  - Node Exporter Full: `http://10.136.1.13:3000/d/rYdddlPWk/node-exporter-full`
- **Prometheus 采集**: 12 个目标（minio-cluster x4, minio-node x4, node-exporter x4）全部 UP
- **MinIO 指标端点**: `/minio/v2/metrics/cluster` 和 `/minio/v2/metrics/node`

### Ray 分布式计算集群

4 节点 Ray 集群，Ray 2.10.0，Head 在 .13。

| 节点 | IP | 角色 | 端口 | CPUs | RAM | GPU |
|------|-----|------|------|------|-----|-----|
| .13 (slave01) | 10.136.1.13 | **Head** | GCS:6379, Dashboard:8265, Client:10001 | 60 | 252GB | 无 |
| .14 (slave02) | 10.136.1.14 | Worker | - | 60 | 252GB | 无 |
| .15 (slave03) | 10.136.1.15 | Worker | - | 60 | 252GB | 无 |
| .20 (inspur) | 10.136.1.20 | Worker | - | 28 | 31GB | 2x Quadro M4000 |

**总计: 208 CPUs, 2 GPUs, 501 GiB 内存**

- **Dashboard**: `http://10.136.1.13:8265`
- **Ray Client**: `ray://10.136.1.13:10001`
- **连接方式**:
  ```python
  import ray
  ray.init(address="ray://10.136.1.13:10001")
  # 或
  ray.init(address="auto")  # 在集群节点上
  ```
- **Systemd 服务**: Head 为 `ray-head.service`，Worker 为 `ray-worker.service`，全部开机自启
- **对象溢出目录**: .13/.14/.15 为 `/data/ray/spill`，.20 为 `/data1/ray/spill`
- **注意事项**:
  - .13 当前 GCS 端口为 6379（Head 已迁移到 .13）
  - .14 不再作为 Head，原有 .14 Head 端口说明已失效
  - .20 的 pip 安装需要 sudo
  - Worker 配置了 `Restart=on-failure`，Head 重启后自动重连
  - 分布式剖分必须使用 `ray` 后端验证，不要只用本地 thread/process 结果代替。
  - 不要用 `RAY_ACTOR_NODE_RESOURCE=node:10.136.1.14` 规避数据路径问题；演示数据已同步到 MinIO，Ray worker 应在各节点本地缓存 `s3://` 源对象后并行处理。
  - Ray runtime env 会排除 `cube_split/data/**`，不要依赖 runtime package 携带大影像数据。
  - 普通光学逻辑剖分（geohash/MGRS）和实体剖分（ISEA4H）都不能让 driver 先生成 `/tmp/.../cog/*.tif` 再交给 Ray worker 读取；不同节点无法访问该本地路径。
  - Worker 侧流程应为：从 MinIO 下载源 TIF 到 `/tmp/cube_split_source_cache`，在 worker 本地转 COG，将 COG/实体瓦片上传回 MinIO，再用 `s3://` 写入 index rows。
  - `s3://` 输出做质检时也要先解析到节点本地缓存后再用 rasterio 打开，不能用 `Path.exists()` 直接判断 MinIO URL。
  - 前端不单独暴露“实体剖分”模块；实体剖分由“光学遥感”页面的剖分格网选择 `ISEA4H` 触发，默认 `grid_level=6`。普通光学/产品逻辑剖分（geohash/MGRS）默认层级仍为 5。
  - 小规模冒烟测试可用 ISEA4H `grid_level=1`、单景影像、`ray_parallelism=2`、`max_cells_per_asset=50`；完整 level 6 任务会占用更多集群 IO 与 CPU。
