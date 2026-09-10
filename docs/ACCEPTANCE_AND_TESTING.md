# 测试与验收

更新时间：2026-09-10

## 1. 测试命令

| 范围 | 命令 |
| --- | --- |
| 全量（三包） | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest` |
| encoder + split | `PYTHONPATH=cube_encoder:cube_split python3.11 -m pytest cube_encoder/tests cube_split/tests` |
| Web 后端 | `cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests` |
| 前端单测 | `cd cube_web/frontend && npm run test:unit` |
| 前端构建 | `cd cube_web/frontend && npm run build` |

在 worktree 内运行时，`PYTHONPATH` 必须指向该 worktree 的包目录，否则可能误导入 site-packages 里的旧 SDK。

## 2. 测试标记与外部依赖

根 `pytest.ini` 注册了三个标记：

| 标记 | 含义 | 依赖 |
| --- | --- | --- |
| `e2e` | 需要外部 Ray、MinIO、PostgreSQL 的端到端用例 | 外部集群 |
| `real_aoi` | 需要可读真实栅格输入 | 环境变量 `CUBE_GRID_REAL_AOI_URI`，未设置时用例直接失败 |
| `quality_repository_opengauss` | 真实 OpenGauss 上的质检仓储验收，**不跳过** | 环境变量 `CUBE_WEB_POSTGRES_DSN`，未设置时用例直接失败 |

`cube_split/tests/` 与 `cube_encoder/tests/` 的其余用例使用打桩与 fixture，不需要外部服务；
`cube_encoder/tests/fixtures/isea4h/` 是 DGGRID 生成的权威向量（带 sha256 清单）。

## 3. 当前门禁状态（2026-09-10）

| 门禁 | 结果 |
| --- | --- |
| 跨包 pytest | 904 passed |
| `real_aoi` 真实栅格验收 | 失败：未设置 `CUBE_GRID_REAL_AOI_URI`（环境依赖，非代码缺陷） |
| OpenGauss 质检仓储验收 | 10 passed（含在全量结果内） |
| 前端单测 | 126 passed |
| 前端构建 | 通过 |

补跑真实栅格门禁：设置 `CUBE_GRID_REAL_AOI_URI` 指向可读栅格后运行
`PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/integration`。

## 4. 编码器性能烟测

```bash
cd cube_encoder
python3.11 -m grid_core.app.perf_smoke
```

共 12 个用例，阈值可用 `PERF_MAX_*` 环境变量覆盖，`PERF_SMOKE_JSON_PATH` 可导出 JSON 结果。
发布前必须执行（见 `cube_encoder/docs/SDK_RELEASE.md`）。

## 5. 验收约定

- 修改 API 行为时同步检查 `cube_web` 调用链并更新测试。
- 涉及前端详情抽屉、弹窗或跨页面复用组件时，打开前必须重置当前记录 id 与状态。
- 推送前运行全量跨包 pytest；Web 相关变更还要在 `cube_web/` 目录单独跑一次。
- 真实环境验收使用真实 Ray 后端，不用本地线程/进程结果代替；真实 `s3://` 对象先用 MinIO `stat_object` 确认存在。
- 历史性能报告保留原始结论并标注测量时间，不得冒充当前契约。
