# 仓库协作指南

## 项目结构与模块边界

本仓库是 Python monorepo，包目录位于仓库根目录。

- `cube_encoder/`：核心格网 SDK 和 API 模型位于 `grid_core/`，测试位于 `tests/`。
- `cube_split/`：剖分、Ray 入库、AOI 读取和作业实现位于 `cube_split/`，测试位于 `tests/`。
- `cube_web/`：FastAPI 后端位于 `cube_web/app.py`，Vue/Vite 前端位于 `frontend/`，测试位于 `tests/`。
- 包级文档放在各包的 `docs/` 目录。

`cube_encoder` 是 SDK 提供方。其他包必须通过 `grid_core.sdk.CubeEncoderSDK`
或 Web SDK backend 使用 encoder 能力，不允许复制格网逻辑。

当前格网契约：`cube_encoder` 完整支持 `s2`、`mgrs`、`tile_matrix`、`isea4h` 的定位、覆盖、拓扑和编码；`plane_grid` 当前只提供 source-plane ST code 校验与 `cube_split` 的逻辑窗口剖分，不是 encoder 的通用 locate/cover/topology 引擎。Web 生产页面不再暴露 `mgrs`，普通逻辑格网使用 `s2`、`tile_matrix`，源 CRS 保留型逻辑剖分使用 `plane_grid`，实体剖分使用 `s2`、`tile_matrix` 或 `isea4h`。

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

- 生产剖分操作和 API 命名使用 `run`。已有 `demo` endpoint 只作为旧客户端兼容别名保留；
  不新增提交 `demo` operation 的生产调用点。
- 内置演示剖分批次必须运行时显式启用。生产启动不得自动 seed 演示批次，除非明确设置
  `CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1`。
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
PR 应包含摘要、影响路径、验证结果、UI 截图，以及可用的关联 issue 或说明。

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
3. 当前工作目录或仓库根目录下的本地 `.cube_web.env`。
4. 代码默认值。

仓库已忽略 `.cube_web.env`。该文件只保留在本地，不能提交凭据。本地部署文件至少包含：

```bash
CUBE_WEB_POSTGRES_DSN=postgresql://<user>:<password>@10.3.100.180:15400/postgres?client_encoding=UTF8
CUBE_WEB_RAY_ADDRESS=10.3.100.182:6379
CUBE_WEB_MINIO_ENDPOINT=10.3.100.179:9000
CUBE_WEB_MINIO_BUCKET=cube
CUBE_WEB_MINIO_ACCESS_KEY=<access-key>
CUBE_WEB_MINIO_SECRET_KEY=<secret-key>
```

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

- **OpenGauss**: 主节点 `10.3.100.180:15400`，database `postgres`，使用 PostgreSQL 兼容 DSN。
- **Ray**: Head/GCS `10.3.100.182:6379`，Dashboard `http://10.3.100.182:8265`。
- **MinIO**: API 可用 `10.3.100.179:9000`、`10.3.100.180:9000`、`10.3.100.181:9000`、`10.3.100.182:9000`，Console `http://10.3.100.181:9001`，bucket `cube`，`secure=false`。

配置页面必须展示 OpenGauss/PostgreSQL 兼容 DSN、Ray 和 MinIO 的运行时启动信息，但不得把这些值写回
`cube_web_configs`。

当 `CUBE_WEB_AUTH_REQUIRED=1` 时，`/v1/partition/schemas/import` 是载入系统使用的公开导入入口；其余 `/v1/*` 默认要求 Bearer Token。前端非管理员只保留公共编码入口，直接访问剖分页面会回到门户首页；这不是后端业务授权的替代品。

OpenGauss 连接变量名仍使用历史兼容名 `CUBE_WEB_POSTGRES_DSN`，代码通过 PostgreSQL
兼容协议和 `psycopg` 连接 OpenGauss。文档和交接说明中应称 OpenGauss，不要把运行库误写成
独立 PostgreSQL 服务。

演示剖分 seed 批次不是生产配置。只有演示环境才应设置：

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

## 基础设施集群信息

### OpenGauss 数据库

4 节点 OpenGauss 7.0.0-RC3 集群，主节点在 `poufennode02`。

| 节点 | IP | 角色 | 端口 |
|------|----|------|------|
| poufennode02 | 10.3.100.180 | **Primary** | 15400 |
| poufennode01 | 10.3.100.179 | Standby | 15400 |
| poufennode03 | 10.3.100.181 | Standby | 15400 |
| poufennode04 | 10.3.100.182 | Standby | 15400 |

- **连接 DSN**: `postgresql://<user>:<password>@10.3.100.180:15400/postgres`
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

### Ray 分布式计算集群

4 节点 Ray 集群，Head 在 `poufennode04`。

| 节点 | IP | 角色 | GCS 地址 |
|------|----|------|----------|
| poufennode04 | 10.3.100.182 | **Head Node** | `10.3.100.182:6379` |
| poufennode01 | 10.3.100.179 | Worker Node | - |
| poufennode02 | 10.3.100.180 | Worker Node | - |
| poufennode03 | 10.3.100.181 | Worker Node | - |

- **Dashboard**: `http://10.3.100.182:8265`
- **GCS 地址**: `10.3.100.182:6379`
- **Head Node IP**: `10.3.100.182`
- **集群 ID**: `3a74cad5b9acda678ec4c7db6bf996772af733039bb153dc4810f131`
- **连接方式**:
  ```python
  import ray
  ray.init(address="10.3.100.182:6379")
  # 或
  ray.init(address="auto")  # 在集群节点上
  ```
- **注意事项**:
  - 分布式剖分必须使用 `ray` 后端验证，不要只用本地 thread/process 结果代替。
- 不要用固定节点资源规避数据路径问题；演示数据应同步到 MinIO，Ray worker 应在各节点本地缓存 `s3://` 源对象后并行处理。
- Ray runtime env 会排除 `cube_split/data/**`，不要依赖 runtime package 携带大影像数据。
- Ray task payload 不得携带 MinIO access key 或 secret key。通过 Ray `runtime_env.env_vars`
  从 worker 运行时环境传入；任务参数只保留业务数据和不敏感的对象定位信息。
- 普通光学逻辑剖分（`s2`/`tile_matrix`）、源 CRS 保留型逻辑剖分（`plane_grid`，当前仅逻辑方式）和实体剖分（`s2`/`tile_matrix`/`isea4h`）都不能让 driver 先生成 `/tmp/.../cog/*.tif` 再交给 Ray worker 读取；不同节点无法访问该本地路径。
- Worker 侧流程应为：从 MinIO 下载源 TIF 到 `/tmp/cube_split_source_cache`，在 worker 本地转 COG，将 COG/实体瓦片上传回 MinIO，再用 `s3://` 写入 index rows。
- 源对象下载缓存必须按 URI 的稳定哈希隔离并校验预期 SHA-256；解析 `s3://` 路径时先
  URL decode，避免中文对象键被二次编码。发生 `ENOSPC` 时只清理该 worker 的
  `/tmp/cube_split_source_cache` 后重试一次，绝不能递归清理通用 `/tmp` 或其他任务目录。
- `s3://` 输出做质检时也要先解析到节点本地缓存后再用 rasterio 打开，不能用 `Path.exists()` 直接判断 MinIO URL。
  - 碳卫星 `run` 任务可以使用 `input_dir` 或 `source_uri`/`selected_observations[].source_uri`；`run` 模式会执行入库，`demo` 兼容入口不得声称或触发入库。
  - 前端不单独暴露“实体剖分”模块；除“碳卫星”外，光学遥感、雷达遥感和信息产品页面都可通过剖分格网选择 `s2`、`tile_matrix` 或 `isea4h`，实体默认 `grid_level=6`。普通逻辑剖分默认层级仍为 5；`plane_grid` 逻辑剖分默认保留源 CRS。`max_cells_per_asset=0` 表示不设上限，smoke/调试任务应显式设置小的正数。
  - `plane_grid` 当前按每个源资产的像素窗口生成 `<crs>/<level>/<col>/<row>` 形式的源平面编码；跨场景唯一性、WGS84 bbox 质检和地图预览仍属于后续重构范围，不得把该模式当作全局拓扑格网使用。
  - 小规模冒烟测试可用 ISEA4H `grid_level=1`、单景影像、`ray_parallelism=2`、`max_cells_per_asset=50`；完整 level 6 任务会占用更多集群 IO 与 CPU。
