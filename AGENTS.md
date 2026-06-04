# 仓库协作指南

## 项目结构与模块边界

本仓库是 Python monorepo，包目录位于仓库根目录。

- `cube_encoder/`：核心格网 SDK 和 API 模型位于 `grid_core/`，测试位于 `tests/`。
- `cube_split/`：剖分、Ray 入库、AOI 读取和作业实现位于 `cube_split/`，测试位于 `tests/`。
- `cube_web/`：FastAPI 后端位于 `cube_web/app.py`，Vue/Vite 前端位于 `frontend/`，测试位于 `tests/`。
- 包级文档放在各包的 `docs/` 目录。

`cube_encoder` 是 SDK 提供方。其他包必须通过 `grid_core.sdk.CubeEncoderSDK`
或 Web SDK backend 使用 encoder 能力，不允许复制格网逻辑。

## 构建、测试与开发命令

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```

运行默认 encoder 和 split 包测试。Web 相关变更还要运行：

```bash
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
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

## 代码风格与命名

保持 Python 3.11 运行兼容，使用 4 空格缩进，公共函数提供类型标注，模块职责保持聚焦。
包名使用小写和下划线，例如 `grid_core`、`cube_split` 和 `cube_web`。测试文件使用
`test_*.py`，测试函数使用描述性的 `test_*` 命名。前端代码保持 plain HTML/CSS/JS
或现有 Vue/Vite 工程风格。

## 执行规则

- 优先做最小有效变更，避免无关重构。
- 除非任务明确要求，不调整跨包公共接口。
- 修改 API 行为时，必须检查 `cube_web` 调用链并同步更新测试。
- 新增依赖前，先确认现有依赖无法满足需求。
- 不随意移动目录或重命名公共模块。

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

Web 启动配置只属于运行时。不要把 PostgreSQL DSN、Ray 地址、MinIO endpoint、门户 URL
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
CUBE_WEB_POSTGRES_DSN=postgresql://<user>:<password>@127.0.0.1:55432/cube
CUBE_WEB_RAY_ADDRESS=ray://10.136.1.13:10001
CUBE_WEB_MINIO_ENDPOINT=10.136.1.14:9000
CUBE_WEB_MINIO_BUCKET=cube
```

当前运行端点：

- **PostgreSQL**: Podman container `cube-pg`, host port `127.0.0.1:55432`, database `cube`.
- **Ray**: `ray://10.136.1.13:10001`.
- **MinIO**: `10.136.1.14:9000`, bucket `cube`, `secure=false`.

配置页面必须展示 PostgreSQL、Ray 和 MinIO 的运行时启动信息，但不得把这些值写回
`cube_web_configs`。

演示剖分 seed 批次不是生产配置。只有演示环境才应设置：

```bash
CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1
```

门户导航属于运行时配置，不属于配置管理数据。默认值为：

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
  PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 - <<'PY'
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

4 节点 Ray 集群，Ray 2.55.x（与 `cube_split` 依赖 `ray>=2.55.1,<2.56.0` 保持一致），Head 在 .13。

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
