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
PYTHONPATH=cube_encoder:cube_split:cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

Runs the web UI with the in-repo SDK and partition backends.

## Coding Style & Naming Conventions

Use Python 3.11+, 4-space indentation, type hints for public functions, and focused modules. Package names are lowercase with underscores, for example `grid_core`, `cube_split`, and `cube_web`. Tests use `test_*.py` files and descriptive `test_*` functions. Keep frontend code in plain HTML/CSS/JS.

## Execution Rules

- Prefer the smallest effective change; avoid unrelated refactors.
- Do not adjust public interfaces across packages unless the task explicitly requires it.
- When changing API behavior, check the `cube_web` call chain and update tests together.
- Before adding dependencies, confirm an existing dependency cannot solve the need.
- Do not casually move directories or rename public modules.

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
