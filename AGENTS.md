# 仓库协作指南

## 项目结构与模块边界

本仓库是 Python monorepo，包目录位于仓库根目录。

- `cube_encoder/`：核心格网 SDK 和 API 模型位于 `grid_core/`，测试位于 `tests/`。
- `cube_split/`：剖分、Ray 入库、AOI 读取和作业实现位于 `cube_split/`，测试位于 `tests/`。
- `cube_web/`：FastAPI 后端位于 `cube_web/app.py`，Vue/Vite 前端位于 `frontend/`，测试位于 `tests/`。
- 包级文档放在各包的 `docs/` 目录。

`cube_encoder` 是 SDK 提供方。其他包必须通过 `grid_core.sdk.CubeEncoderSDK`
或 Web SDK backend 使用 encoder 能力，不允许复制格网逻辑。

当前生产格网契约严格限定为 `geohash`、`mgrs`、`isea4h`。三者均由
`cube_encoder` 提供定位、覆盖、拓扑和编码能力；`geohash`、`mgrs` 固定使用逻辑剖分，
`isea4h` 固定使用实体剖分。Web、`cube_split` 和 SDK 不得暴露或接受
`s2`、`tile_matrix`、`plane_grid` 等历史格网作为生产入口。

## 构建、测试与开发命令

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```

运行默认 encoder 和 split 包测试。Web 相关变更还要运行：

```bash
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

仓库根目录的 `pytest.ini` 已把 `cube_encoder/tests`、`cube_split/tests` 和
`cube_web/tests` 都列入 `testpaths`。需要全量回归时可运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

```bash
cd cube_encoder && python3.11 -m build
```

构建 `cube-encoder` 分发包。

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039
```

使用 Python 3.11、仓库内 SDK 和剖分后端运行 Web API。匹配环境变量未设置时，
运行时服务端点会从本地 `.cube_web.env` 自动加载。

```bash
cd cube_web/frontend && npm run dev
```

前端开发服务运行在 `50040`，并把 `/v1`、`/api` 和 `/health` 代理到后端 `50039`。
前端代码变更至少运行：

```bash
cd cube_web/frontend && npm run build
```

## 代码风格与命名

保持 Python 3.11 运行兼容，使用 4 空格缩进，公共函数提供类型标注，模块职责保持聚焦。
包名使用小写和下划线，例如 `grid_core`、`cube_split` 和 `cube_web`。测试文件使用
`test_*.py`，测试函数使用描述性的 `test_*` 命名。前端代码保持 plain HTML/CSS/JS
或现有 Vue/Vite 工程风格。

## 执行规则

- 优先做最小有效变更，避免无关重构。
- 除非任务明确要求，不调整跨包公共接口。
- 修改 API 行为时，必须检查 `cube_web` 调用链并同步更新测试。
- 修改前端详情抽屉、弹窗或跨页面复用组件时，必须先重置当前记录 id 或状态，避免复用上一次打开的详情数据。
- 新增依赖前，先确认现有依赖无法满足需求。
- 不随意移动目录或重命名公共模块。
- 文档中的运行端点、格网矩阵、认证规则和数据表语义必须以代码与测试为准；历史性能报告保留原始结果，但必须标注测量时间，不得冒充当前契约。

## 生产与演示分离

`master`/`main` 保持为生产开发基线。生产代码负责可复用剖分执行链路、托管批次流程、
重试/取消/质检行为、运行时配置和测试。

- 生产剖分操作和 API 命名统一使用 `run`，不保留 `demo` 生产 endpoint。
- 演示批次只在 `demo/*` 分支存在且必须显式启用（`CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1`）；
  `master` 已无演示 seed 代码（`load_demo_partition_schemas()` 于 2026-07-18 `ba5741b` 删除），
  生产启动不得自动 seed 演示批次。
- 演示专用文档、本地 `.cube_web.env` 示例、seed 数据 manifest、冒烟编排和汇报脚本只放在
  `demo/*` 分支，例如 `demo/partition-chain-202606`。
- 不把演示数据、本地绝对数据路径、凭据或演示专用硬编码 source manifest 合回生产分支。
- 通用 bug 修复和可复用剖分能力从生产分支流向演示分支。演示专用调整留在演示分支。
  如果演示运行暴露真实生产 bug，应提取最小修复并 cherry-pick 或 PR 回生产分支。

## 测试规则

项目使用 `pytest`。新增或修改测试应放在对应包旁边。SDK/API 变更需要覆盖服务行为，
必要时覆盖 FastAPI endpoint。推送前运行上面的完整跨包 pytest；窄范围 Web 变更还要在
`cube_web/` 目录运行：

```bash
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

## Commit 与 PR 规则

近期历史使用简短祈使句 commit message，有时带 `feat:`、`docs:` 或 `feat(partition):`
等前缀。保持 commit 聚焦且用户可理解，例如 `Update cube web SDK backend and UI`。

GitHub 发布使用 `gh` CLI。每次 push 前运行完整跨包 pytest，并在 PR 或交接说明中包含结果。
PR 应包含摘要、影响路径、验证结果和 UI 截图。

如果沙箱内 `gh` 认证或 GitHub CLI 访问失败，先在沙箱外提权重试同一个 `gh` 命令，再判断认证无效。

## 安全与配置提示

不要提交本地数据、缓存、`.pytest_cache/`、`__pycache__/`、虚拟环境或大型入库输入。
服务端点保持可配置，避免在业务代码中硬编码机器专属 IP。

## Web 运行时配置

Web 启动配置只属于运行时。不要把 OpenGauss 兼容 DSN、Ray 地址、MinIO endpoint、门户 URL
或凭据存入 `cube_web_configs` 表。该表只保存用户可编辑的业务默认值：

- `partition`
- `ingest`
- `quality`

`cube_split.runtime_config` 按以下顺序解析运行时值：

1. 进程环境变量。
2. 已设置的 `CUBE_WEB_ENV_FILE`。
3. 当前工作目录、用户主目录（`~`）或仓库根目录下的本地 `.cube_web.env`（按该顺序取第一个有值的文件）。
4. 代码默认值。

仓库已忽略 `.cube_web.env`。该文件只保留在本地，不能提交凭据。本地部署文件至少包含：

```bash
CUBE_WEB_POSTGRES_DSN=postgresql://<user>:<password>@10.3.100.180:15400/<database>?client_encoding=UTF8
CUBE_WEB_PARTITION_EXECUTOR=ray_job
CUBE_WEB_RAY_JOB_ADDRESS=http://10.3.100.183:30826
CUBE_WEB_RAY_ADDRESS=auto
CUBE_WEB_MINIO_ENDPOINT=10.3.100.179:9000
CUBE_WEB_MINIO_BUCKET=cube
CUBE_WEB_MINIO_ACCESS_KEY=<access-key>
CUBE_WEB_MINIO_SECRET_KEY=<secret-key>
```

`CUBE_WEB_PARTITION_EXECUTOR` 默认 `local`；生产 Ray 执行必须显式设为 `ray_job` 并提供
`CUBE_WEB_RAY_JOB_ADDRESS`。`CUBE_WEB_RAY_ADDRESS=auto` 只对在集群内运行的 Ray Job driver 有效，
不要把 NodePort 或节点地址写进该变量。KubeRay 细节见 `cube_web/docs/KUBERAY_OPERATIONS.md`。

日志属于运行时配置，同样只通过环境变量或本地 `.cube_web.env` 控制，不写入
`cube_web_configs`：

- `CUBE_LOG_LEVEL`：根日志级别，默认 `INFO`。
- `CUBE_LOG_FORMAT`：`text`（默认）或 `json`。
- `CUBE_LOG_FILE`：设置后写入该文件并按大小轮转，默认 10MB × 3，可用
  `CUBE_LOG_MAX_BYTES` / `CUBE_LOG_BACKUP_COUNT` 调整；不设置时只输出 stdout。
  该值不会下发到 Ray worker（worker 只配置 stdout）。
- `CUBE_LOG_RAY_LEVEL`：Ray 自身日志级别，默认 `ERROR`，可设 `WARNING`/`INFO` 放开。
- `CUBE_LOG_ACCESS`：`0` 时关闭应用访问日志。

### 隔离 worktree 与真实门禁

- 新里程碑 worktree 必须从协调账本中已通过的前置 `integration_hash` 创建。不要从
  旧验收 worktree、主 checkout 的脏状态或另一里程碑的 worker 分支开始。
- `CUBE_WEB_ENV_FILE` 由 Python 运行时配置代码解析；它不会自动向 shell 导出变量。运行
  需要 shell 环境变量的重置或真实门禁脚本时，使用受控子 shell 显式加载本地文件，例如：
  ```bash
  set -a
  . "${CUBE_WEB_ENV_FILE:-$PWD/.cube_web.env}"
  set +a
  ```
  不要打印该文件、`env` 全量输出、DSN 或凭据。
- 真实门禁可以有测试专用变量，但不得将其写入 `.cube_web.env`、业务配置表或源码。脚本
  应优先使用正式变量名；若历史测试要求 `RAY_ADDRESS` 而运行时使用
  `CUBE_WEB_RAY_ADDRESS`，只在该次受控命令中显式映射。真实 `s3://` COG 必须先用 MinIO
  `stat_object` 验证存在，再传入门禁。
- 执行 `reset_partition_domain.py --execute` 前，必须同时指定
  `CUBE_WEB_ENV=development`、`--dangerously-reset-partition-domain` 和与 DSN 实际连接
  数据库完全相同的 `--database-name`。先 preview，确认对象清单只属于授权的领域表；重置
  成功不等同于真实门禁通过，仍须完成所有 non-skipping 场景。
- 在 worktree 内运行测试时，`PYTHONPATH` 必须指向该 worktree 的包目录。以
  `cube_web/` 为当前目录时使用 `../cube_encoder:../cube_split:.`；错误的相对路径可能静默
  导入用户 site-packages 中的旧 SDK。
- 生成最终集成补丁前，待纳入的新增文件必须先 `git add`；`git diff <base>` 不会包含
  未跟踪文件。最终干净集成 worktree 应从前置哈希重建并执行 `git apply --index` 和
  `git diff --cached --check`，以避免把协调 worktree 的无关改动带入单一里程碑提交。

当前运行端点：

- **OpenGauss**: 主节点 `10.3.100.180:15400`，database 由 DSN 指定（当前本地部署为 `cube_v3`），使用 PostgreSQL 兼容 DSN。
- **Ray（KubeRay）**: Jobs API `http://10.3.100.183:30826`（Web 的任务提交入口），GCS NodePort `10.3.100.183:30637`（外部访问入口，不能当作 `CUBE_WEB_RAY_ADDRESS`）；head 为 `--num-cpus=0`，集群内作业用 `CUBE_WEB_RAY_ADDRESS=auto`。
- **历史（已弃用）**: 裸机 Ray `10.3.100.182:6379` / Dashboard `http://10.3.100.182:8265`（2026-09-13 起不再用于生产与测试，仅留作历史记录）。
- **MinIO**: API 可用 `10.3.100.179:9000`、`10.3.100.180:9000`、`10.3.100.181:9000`、`10.3.100.182:9000`，Console `http://10.3.100.181:9001`，bucket `cube`，`secure=false`。

配置页面必须展示 OpenGauss/PostgreSQL 兼容 DSN、Ray 和 MinIO 的运行时启动信息，但不得把这些值写回
`cube_web_configs`。

鉴权默认开启。`/v1/partition/schemas/import` 是载入系统使用的公开导入入口；`/v1/client-errors` 为可选鉴权（匿名可用，用于登录前上报浏览器错误）；其余 `/v1/*` 默认要求 Bearer Token。只有受控本地测试才可显式设置 `CUBE_WEB_AUTH_REQUIRED=0`。前端非管理员只保留公共编码入口，直接访问剖分页面会被路由回编码页（`encoding`）；这不是后端业务授权的替代品。

OpenGauss 连接变量名仍使用历史兼容名 `CUBE_WEB_POSTGRES_DSN`，代码通过 PostgreSQL
兼容协议和 `psycopg` 连接 OpenGauss。文档和交接说明中应称 OpenGauss，不要把运行库误写成
独立 PostgreSQL 服务。

演示剖分 seed 批次不是生产配置，`master` 上没有任何代码读取该开关（演示 seed 代码只在
`demo/*` 分支）。只有 `demo/*` 分支的演示环境才设置：

```bash
CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1
```

门户导航属于运行时配置，不属于配置管理数据。默认值为：

- 首页: 由 `CUBE_WEB_PORTAL_HOME_URL` 设置。
- 剖分数据服务: 由 `CUBE_WEB_PORTAL_PARTITION_SERVICE_URL` 设置。
- 资源调度: 由 `CUBE_WEB_PORTAL_DISPATCH_URL` 设置。
- ARD数据载入: 由 `CUBE_WEB_PORTAL_DATA_INGEST_URL` 设置。
- 后台管理: 由 `CUBE_WEB_PORTAL_ADMIN_URL` 设置。

---

## 性能优化与真实库验证

性能工作按“先定位、后优化、再证等价”三步走，禁止凭感觉改热点路径。

### 测量纪律

- **硬证据优先**：格元数、行数、脏页数、逐字节一致数、执行计划节点（如 `Index Only Scan`）与机器负载无关，可以作为结论；计时只在同一次测量内部用于排序。
- 计时一律报**多轮最小值 + 中位**，并标注测量时的机器负载。A/B 对比必须**轮转执行顺序**（否则“总是最后测”的变体会虚高 20–30%，本项目已据此撤回过硬结论）。
- 优化前先证伪瓶颈假设：已出现多次“看着像瓶颈、实测不是”（`_verify_minio_objects` 串行 stat 仅 0.21 s；快照 SQL 服务端仅 1.1 s，其余时间在客户端行传输与物化）。
- 语义等价的改动优先于“需要新语义”的改动；等价性必须有可复现证据（代数恒等、由 ON 键唯一确定、逐字节/逐格元比对、格元数逐例一致）。

### 真实库探针安全模板（必须遵守）

- 只写 scratch schema，影子表**与生产表同名**（否则 `MERGE`/`UPDATE` 里的无限定表名会回落到 `public` 的真表），并在同一会话 `SET search_path = <scratch>, public`。
- 执行前后必须断言：`SELECT oid FROM pg_class WHERE oid = '<表名>'::regclass` 等于影子表 OID（OpenGauss 没有 `to_regclass`）；写入前后核对生产表行数不变。
- 探针禁止把 DSN / MinIO 凭据打印到终端或写入文档，只输出 `bool(os.environ.get(...))` 这类存在性判断。
- 归档证据：`~/perf-probe-evidence-20260912/`（`/tmp/verify-grid/probe_guard.py` 是临时模板，清理后可能不存在，需按上述规则重建）。

### 已落地优化的语义边界

- `rs_cube_cell_fact` 的 MERGE **不再在 UPDATE 分支重写 `cell_geom`**（几何由 ON 键中的 `grid_type/grid_level/space_code` 唯一确定；实测 20k 行全 MATCHED 时 min −30.6%）。因此**同一 `cube_version` 重跑不再修复“非 NULL 但过期”的几何**；`cell_geom IS NULL` 的历史行由 `_backfill_missing_cell_geom` 兜底。几何修正的正规出口是：新的 `cube_version`、迁移脚本，或**手动入库**（见下）。
- `_load_snapshot` 按 `(dataset_id, output_version)` **一次性取格元再回填**，不再逐行 JOIN TOAST 几何（实测 70,909 索引行 : 248 格元时 8.71 s → 5.30 s）；JOIN 仍保留作“索引行必须有格元”的过滤，格元缺失立即报错。
- 改写入/快照路径前必读：`cube_split/cube_split/ingest/managed_output_ingest._verify_targets` 用 `run_id = job_id AND cube_version` 计数且要求 `cell_geom IS NOT NULL`，**任何“跳过写入”的优化都必须同步调整该口径**，否则 managed ingest 自检失败。
- 手动入库（`POST /v1/datasets/{id}/ingest`、`request_manual_ingest_collection`）是显式、低频、有质量门禁与审计的操作，适合承担昂贵转换/修复：几何修复应按**不同格元**比较（`ST_Equals`），成本 ∝ 格元数而非行数。注意 `manual=True` 只放宽资格规则、**不落库到 ingest run**，且选单元 SQL 明确排除 `ingest_status='completed'`，因此“重新入库已完成的单元”需要补能力（产品语义决定）。

### 已知结构性问题（未授权前不要动）

- `rs_cube_cell_fact` 累计 **134 万次非 HOT 更新**（2026-09-13 测量 `n_tup_upd≈1.34M`；2026-09-18 只读核对为 `n_tup_upd=1,344,636`、`n_tup_ins=533,960`、`n_tup_hot_upd=2,124`（0.158%）），来源是重复 ingest：16 个版本对应 214 次作业，同一 `output_version` 最多重跑 60 次。`fillfactor=80` 已于 2026-09-13 设置，HOT 命中仍无明显改善；继续降低需要维护窗口重写表或改 `_verify_targets` 口径。
- 该表 heap 曾膨胀到 410 MB / 6 万活行；2026-09-13 已执行 `fillfactor=80` + `VACUUM FULL` + 相关表 `REINDEX/ANALYZE`，当前 `pg_relation_size≈99.8 MB` / 8.8 万活行（2026-09-17 只读核对）。后续 `VACUUM FULL`/`REINDEX` 仍属破坏性运维，需明确授权。
- 性能记录与复现命令：`docs/PERFORMANCE_OPTIMIZATION_STEPS_20260913.md`（方法+收益+撤回项）、`docs/GRID_OPTIMIZATION_ROUND1_20260912.md`、`docs/PARTITION_WRITE_PERFORMANCE_HANDOFF.md`、`docs/PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md`。

---

## 基础设施集群信息

### OpenGauss 数据库

4 节点 OpenGauss 7.0.0-RC3 集群，主节点在 `poufennode02`。

| 节点 | IP | 角色 | 端口 |
|------|----|------|------|
| poufennode02 | 10.3.100.180 | **Primary** | 15400 |
| poufennode01 | 10.3.100.179 | Standby | 15400 |
| poufennode03 | 10.3.100.181 | Standby | 15400 |
| poufennode04 | 10.3.100.182 | Standby | 15400 |

- **连接 DSN**: `postgresql://<user>:<password>@10.3.100.180:15400/<database>`（database 由 DSN 指定，当前本地部署为 `cube_v3`）
- **凭据来源**: 运行时从环境变量、`CUBE_WEB_ENV_FILE` 或本地 `.cube_web.env` 读取；不要把明文口令写入仓库。
- **兼容说明**: 代码变量和部分错误信息沿用 PostgreSQL 命名，但实际目标库是 OpenGauss。

### MinIO 分布式集群

4 节点分布式集群，每个节点提供 API `:9000` 和 Console `:9001`。

| 节点 | IP | 数据目录 | MinIO API | Console |
|------|----|----------|-----------|---------|
| poufennode01 | 10.3.100.179 | `/data/minio` | `:9000` | `:9001` |
| poufennode02 | 10.3.100.180 | `/data/minio` | `:9000` | `:9001` |
| poufennode03 | 10.3.100.181 | `/data/minio` | `:9000` | `:9001` |
| poufennode04 | 10.3.100.182 | `/data/minio` | `:9000` | `:9001` |

- **Console 入口**: `http://10.3.100.181:9001`
- **默认 API Endpoint**: `10.3.100.179:9000`
- **可用 API Endpoint**: `10.3.100.179:9000`、`10.3.100.180:9000`、`10.3.100.181:9000`、`10.3.100.182:9000`
- **认证来源**: 运行任务时优先从 `CUBE_WEB_MINIO_ACCESS_KEY` / `CUBE_WEB_MINIO_SECRET_KEY`、`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` 或节点本地 MinIO 服务环境读取，不在仓库记录明文口令。
- **演示源数据前缀**:
  - 光学/实体剖分源影像: `s3://cube/cube/source/optocal/...`
  - 碳卫星源数据: `s3://cube/cube/source/carbon/...`
  - 信息产品源影像: `s3://cube/cube/source/product/...`
  - 前端 demo schema 的 `source_uri` 应使用上述 `s3://` URL，不要回退为某一台机器的本地绝对路径。
- **源数据同步命令参考**:
  ```bash
  PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
  from concurrent.futures import ThreadPoolExecutor, as_completed
  from pathlib import Path
  from minio import Minio
  from minio.error import S3Error

  root = Path.cwd()
  jobs = [
      (root / "cube_split/data/product", "cube/source/product"),
      (root / "cube_split/data/optocal", "cube/source/optocal"),
  ]
  client = Minio("10.3.100.179:9000", access_key="...", secret_key="...", secure=False)
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

### Ray 分布式计算集群（KubeRay）

生产 Ray 走 **KubeRay**，集群 `cube-partition`（namespace `kuberay-system`），不是任何裸机集群。

| 项 | 值 |
|---|---|
| RayCluster CR | `kuberay-system/cube-partition`（KubeRay operator chart `kuberay-operator-1.3.0`） |
| Head Pod | `cube-partition-head-*`（2 CPU/8Gi 申请；`ray start --head --num-cpus=0` → **head 不提供 CPU**） |
| Worker 组 | 两组均由 autoscaler 动态扩缩（`minReplicas=0`）：`partition-workers` **max=14** / 每 pod **1 CPU / 2Gi** / 声明 `cube_partition_worker: 1`；`partition-workers-large` **max=5** / 每 pod **1 CPU / 4Gi** / 声明 `cube_partition_worker: 1` + `cube_partition_worker_large: 1`。实体（`isea4h`）任务用 `CUBE_ENTITY_NODE_RESOURCE=cube_partition_worker_large` 固定到大内存组。 |
| 空闲回收 | 每组 `idleTimeoutSeconds=1800`；顶层 `autoscalerOptions.idleTimeoutSeconds=600` 仅作回退 |
| **Jobs API（推荐接入方式）** | `http://10.3.100.183:30826`（dashboard 端口 8265 的 NodePort；`/api/jobs/` 可查历史作业） |
| GCS NodePort | `10.3.100.183:30637`（→ head 的 6379） |
| 运行时镜像 | `10.3.100.183:30500/remote-sensing/cube-kuberay-runtime:20260728` |
| worker 环境 | 通过 `cube-runtime-secret` 的 `envFrom` 自带 `CUBE_WEB_MINIO_*` / `CUBE_WEB_POSTGRES_DSN`；`CUBE_WEB_RAY_ADDRESS` 由 Web ray_job 提交器的 `runtime_env` 传入，**不需要往仓库或任务 payload 注入凭据** |
| worker 可用库 | `rasterio`/`minio`/`psycopg`/`shapely`/`numpy`/`pyproj` 可导入；**`fiona` 缺失** |

- **提交方式**（driver 在集群内起，autoscaler 才会扩容）：
  ```bash
  ray job submit --address http://10.3.100.183:30826 --working-dir <dir> --no-wait -- python <script.py>
  ```
- **ray_job 转发内容**：`cube_web/services/ray_job_submitter.py` 在基础 runtime env（主机设置了 `CUBE_SOURCE_CACHE_DIR` 时会带上）之外，注入 `CUBE_WEB_RAY_JOB_DRIVER`、`CUBE_WEB_POSTGRES_DSN`、`CUBE_WEB_RAY_ADDRESS`、`CUBE_WEB_MINIO_ENDPOINT`/`ACCESS_KEY`/`SECRET_KEY`/`BUCKET`，并按固定白名单透传 `CUBE_WEB_RAY_BATCH_SCHEDULER`、`CUBE_ENTITY_*`（`RAY_PARALLELISM`/`BANDS_PER_TASK`/`UPLOAD_WORKERS`/`MINIO_PARALLEL_UPLOADS`/`NODE_RESOURCE`/`READ_BLOCK_PIXELS`/`TILE_TMP_DIR`）和 `CUBE_WEB_RAY_WORKER_RESOURCE`；`CUBE_LOGICAL_*`、`RAY_ACTOR_NODE_RESOURCE` **不在转发白名单内**（2026-09-17 确认）：写在 Web 主机 `.cube_web.env` 里的覆盖值不会进入 ray_job 作业，集群内 driver 仍按自己的环境变量取值（未设置时用代码默认 4 / 1 / 16）；要覆盖需在提交作业的 runtime_env 里显式传入。
- **在集群内/作业内**用 `ray.init(address="auto")` 是正确的（`.cube_web.env` 的 `CUBE_WEB_RAY_ADDRESS=auto` 就属于这种用法）。
- **不要**把 NodePort 地址写进 `CUBE_WEB_RAY_ADDRESS`：head 是 `--num-cpus=0`，**外部 `ray.init(address="<节点>:30637")` 在 0 worker 时必报 `No node info found matching attributes`**；即使 worker 已拉起，从集群外主机实测仍然连不上，所以外部 driver 不是支持的接入路径。
- **跑测试或生产作业前先预热**：先确认有 Running 的 `cube-partition-partition-workers-worker-*`（`kubectl -n kuberay-system get pods`），或按本节末尾的预热 job 拉起 worker。
- 实体剖分读取是**分块流式**的：窗口像素超过 `CUBE_ENTITY_READ_BLOCK_PIXELS`（默认 4,000,000 px）时逐块读取 + 掩膜，写入 `CUBE_ENTITY_TILE_TMP_DIR` 临时 GeoTIFF 后流式上传；实测峰值内存 1.62 GB → 422 MB（2026-09-17，演示雷达单景上覆盖整景的粗 isea4h 格元窗口 34,025×15,457 px；1.62 GB 是优化前一次性读取全部波段的开销），优化前后产物文件字节数相同、像素一致（525.9 Mpx 中 0 个像素不同）。该变量在 ray_job 白名单内，可运行时覆盖。
- **注意事项**:
  - 分布式剖分必须使用 `ray` 后端验证，不要只用本地 thread/process 结果代替。
- 不要用固定节点资源规避数据路径问题；演示数据应同步到 MinIO，Ray worker 应在各节点本地缓存 `s3://` 源对象后并行处理。
- Ray runtime env 会排除 `cube_split/data/**`，不要依赖 runtime package 携带大影像数据。
- Ray task payload 不得携带 MinIO access key 或 secret key。通过 Ray `runtime_env.env_vars`
  从 worker 运行时环境传入；任务参数只保留业务数据和不敏感的对象定位信息。
- 普通光学逻辑剖分（`geohash`/`mgrs`）和实体剖分（`isea4h`）都不能让 driver
  先生成 `/tmp/.../cog/*.tif` 再交给 Ray worker 读取；不同节点无法访问该本地路径。
- **源数据已经是 COG，剖分链路不再做 COG 转换**（2026-09-13 确认）：worker 侧只调 `cube_split/jobs/ray_partition_core.cache_source_cog` 把源 COG **原样**缓存到 `/tmp/cube_split_source_cache`（按 URI 的 sha256 分目录，带 stat 身份校验与 ENOSPC 回退），随后用 `s3://` 写 index rows。因此不要再按“下载 TIF → 转 COG → 上传”描述这条链路，也不要把 target-crs / 压缩参数当作剖分的必经步骤。
- 入库侧（postgres 元数据）走 `asset_storage_backend=minio` → `upload_assets_to_minio`；仅 sqlite 本地开发才用 `materialize_cog_assets`。评估入库耗时时要看这一步是否在同 Bucket 内做了多余的整文件拷贝（可用服务端 copy 代替）。
- 源对象下载缓存必须按 URI 的稳定哈希隔离并校验预期 SHA-256；解析 `s3://` 路径时先
  URL decode，避免中文对象键被二次编码。发生 `ENOSPC` 时只清理该 worker 的
  `/tmp/cube_split_source_cache` 后重试一次，绝不能递归清理通用 `/tmp` 或其他任务目录。
- `s3://` 输出做质检时也要先解析到节点本地缓存后再用 rasterio 打开，不能用 `Path.exists()` 直接判断 MinIO URL。
  - 碳卫星 `run` 任务使用 Scene 资产中的 `source_uri`，保留 NetCDF/HDF5 原始数据，不转换为 COG。
  - 前端不单独暴露“实体剖分”模块；光学遥感、雷达遥感和信息产品页面通过格网
    类型选择 `geohash`、`mgrs` 或 `isea4h`，剖分方式由格网类型派生，不允许用户混选。
    `max_cells_per_asset=0` 表示不设上限，smoke/调试任务应显式设置小的正数。
  - 小规模冒烟测试可用 ISEA4H `grid_level=1`、单景影像、`ray_parallelism=2`、`max_cells_per_asset=50`；完整 level 6 任务会占用更多集群 IO 与 CPU。
- **当前性能目标（2026-09-13 起，已达标）**：单景全部波段，**从剖分到入库 < 10 s**。范围限定为两种**逻辑格网**；**六边形/实体剖分（`isea4h`）不在测试与优化范围内**。
  - 最终口径：**geohash L4（~35 格元）5.4–7.3 s、mgrs L0（~9 格元）5.6–6.1 s**，均稳定达标（KubeRay、worker 已预热、DB 时钟、单景 4 波段）。
  - **MGRS 层级由调用方请求更粗的 L0，服务端不做层级归一化**（曾实现自动上选一层，因“生效配置 ≠ 请求层级”破坏 2 个既有测试而回退）；MGRS 格元尺寸 = `100 km / 10^level`。
  - 经验边界：每景格元数 ≲ 300 时可稳定 < 10 s，之后按毫秒/行线性增长（mgrs L1 165–182 格元 9.3–14.6 s；geohash L5 720 格元 19.4 s）。
  - 计量口径：必须先预热 worker（否则 autoscaler 冷启动约 90 s，不计入剖分时间；8 任务预热 job 总耗时实测 125 s），报告分两段——**作业自身耗时**（剖分 / 质检 / 入库分段）与**含冷启动的端到端耗时**。
  - 完整记录与复现：`docs/PERF_SINGLE_SCENE_10S_20260913.md`。
- **跑分布式剖分/落库前先做只读健康检查**：`kubectl -n kuberay-system get pods` 里要有 Running 的 `cube-partition-partition-workers-worker-*`（head 本身 `--num-cpus=0`，不提供算力）。
- **历史遗留（已弃用，2026-09-13 起）**：`10.3.100.182:6379` 的裸机 Ray（4 台 poufennode，session 从 2026-07-26 起）仍有残留 DEAD 节点记录（`ray.nodes()` 10 条、存活 4 条、`cluster_resources().CPU` 报 48），误连它会遇到 `Failed to startup worker after retrying 5 times` / `Failed to connect to socket at /tmp/ray/session_*/sockets/raylet`。它不再用于生产与测试，也不要拿它的数字做性能基线；旧 AGENTS.md 记录的裸机端点与集群 ID 一律作废。
- **预热 worker（生产作业前）**：提交一个小 job 驱动 autoscaler 扩容（每组 `idleTimeoutSeconds=1800`）：
  ```bash
  ray job submit --address http://10.3.100.183:30826 --working-dir <dir> --no-wait -- python <warm.py>
  ```
  每个任务做：导入 `rasterio/minio/psycopg/shapely`、建 `/tmp/cube_split_source_cache`、回报节点名与环境变量存在性。2026-09-13 实测 8 任务 125 s 完成、拉起 8 个 worker（低负载 8/8 全成）。
- `.cube_web.env` 里的 `CUBE_WEB_RAY_ADDRESS=auto` 对“在集群内跑的 driver/作业”是正确的；**不要把 NodePort 或任何节点地址写进去**。
- K8s 侧排查/重启用 `kubectl`（本机已装 v1.35.0）：`kubectl get rayclusters -A`、`kubectl get pods -A | grep -i ray`；运维说明见 `docs/rag/10-技术手册/T4-资源调度-技术手册.md` 与 `docs/rag/00-总览/02-部署拓扑与运行实例.md`。
