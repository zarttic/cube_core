# 我方执行清单

## 先完成的交付

- [ ] 使用 `templates/Dockerfile` 构建镜像并推送到业务方指定的私有镜像仓库。镜像 tag 使用 Git commit hash，部署时固定到 digest。
- [ ] 向业务方提供镜像名、digest、Python 3.11 和 Ray `2.55.x` 兼容性说明。
- [ ] 提供任务入口：`python -m cube_web.jobs.partition_batch_job --task-id <task-id>`。
- [ ] 提供资源估算：普通光学逻辑剖分、ISEA4H 实体剖分和碳卫星任务分别需要的 CPU、内存、临时盘和预期并发。
- [ ] 提供 MinIO 输入前缀、输出前缀和 OpenGauss 所需表权限；不提供或记录真实密码。

## 部署配置

业务方完成 Head Service 后，由部署 `cube_web` 的环境设置：

```text
CUBE_WEB_PARTITION_EXECUTOR=ray_job
CUBE_WEB_RAY_JOB_ADDRESS=http://<ray-head-service>.<namespace>.svc:8265
RAY_RUNTIME_ENV_JSON={"env_vars":{"CUBE_SOURCE_CACHE_DIR":"/data/cube_split_source_cache"}}
```

`CUBE_WEB_RAY_JOB_ADDRESS` 是 Web 服务调用 Ray Jobs API 的地址。它不是 GCS 地址，也不是节点 IP。

`CUBE_WEB_RAY_ADDRESS` 目前仍被代码用于 driver 和健康检查。迁移时先由平台方确认其可用地址；建议在完成下述改造后，将 Web 侧地址与 Job driver 内的 `auto` 地址分离。

## 上线前必须修改的代码

当前实现会在提交时把 OpenGauss DSN 及 MinIO 密钥写入 Ray `runtime_env.env_vars`。涉及：

- `cube_web/cube_web/services/ray_job_submitter.py`
- `cube_web/cube_web/services/partition_dataset_runner.py`
- `cube_split/cube_split/partition/carbon.py`

上线前应完成以下最小改造，并新增回归测试：

1. 删除这些路径对 `CUBE_WEB_POSTGRES_DSN`、`CUBE_WEB_MINIO_ACCESS_KEY`、`CUBE_WEB_MINIO_SECRET_KEY` 的 runtime-env 注入。
2. 让 Head 和 Worker Pod 使用 `envFrom.secretRef` 注入运行时密钥；应用继续经既有 `runtime_config` 读取环境变量。
3. 新增独立配置，例如 `CUBE_WEB_RAY_JOB_DRIVER_ADDRESS`。Ray Job driver 设为 `auto`，Web 服务保留 Head Service 的可达连接地址。
4. 镜像化后通过 `RAY_RUNTIME_ENV_JSON` 传入不含 `working_dir` 的最小环境，避免 Ray 上传本地仓库。

在以上改造完成前，只能将 KubeRay 用于隔离的开发验证，不能作为生产凭据边界。

## 我方验收证据

- [ ] 镜像扫描、镜像 digest 和依赖清单。
- [ ] RayCluster Head/Worker 健康状态与 Worker 数量。
- [ ] 一个成功的 Ray Job ID 和对应日志。
- [ ] MinIO 源对象 `stat_object` 成功记录和输出对象列表。
- [ ] OpenGauss 中任务状态、剖分结果与质检状态。
- [ ] 取消、失败和重试各一条受控测试记录。
