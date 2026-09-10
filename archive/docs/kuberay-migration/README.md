# Cube 项目 KubeRay 迁移包

本目录只包含迁移说明和部署模板，不会改变现有 `cube_encoder`、`cube_split`、`cube_web` 的运行代码。

## 推荐目标架构

`cube_web` 创建并持久化剖分任务 -> Ray Jobs API -> KubeRay 管理的长期 `RayCluster` -> Ray Worker Pod 从 MinIO 读取输入、写回 MinIO 和 OpenGauss。

选择长期 `RayCluster` 是因为 Web 已经通过 `RayJobPartitionSubmitter` 提交独立 Ray driver，并能把 Ray Job ID 回写到任务记录。不要为每次前端剖分都创建一套临时集群，除非业务方明确要求使用 `RayJob` CRD 的独占集群模式。

## 文件导航

- [我方执行清单](01-our-work.md)：本项目维护方要做什么，按顺序执行。
- [业务方交接单](02-business-handoff.md)：发送给 K8s/KubeRay 平台方的需求清单。
- `templates/Dockerfile`：用于构建包含全部 Cube Python 包的运行镜像。
- `templates/raycluster.yaml`：长期 KubeRay 集群的起始模板。
- `templates/runtime-env.json`：镜像已包含代码时使用的最小 Ray runtime environment。

## 不要这样做

- 不使用 `cube_split/scripts/start_ray_head.sh` 或 `start_ray_worker.sh` 启动生产集群。
- 不通过 `scp`、共享源码目录或每次任务上传仓库来部署生产代码。
- 不把 OpenGauss DSN、MinIO access key 或 secret 写入 Git、ConfigMap、Ray `runtime_env` 或任务 payload。
- 不把大影像、COG 或 NetCDF 放进镜像或 Ray `working_dir`；它们继续使用 MinIO `s3://` URI。

## 建议的验收顺序

1. 平台方部署模板的一个小规格 RayCluster，Head 和 Worker 都能从镜像仓库拉取镜像。
2. 在 Pod 中验证能访问 MinIO、OpenGauss，以及 DNS 能解析 Ray Head Service。
3. 部署 `cube_web`，配置为使用 Ray Jobs API，并提交一个无敏感数据的小任务。
4. 使用单景 ISEA4H level 1、`ray_parallelism=2`、`max_cells_per_asset=50` 运行完整剖分冒烟。
5. 验证 MinIO 输出、OpenGauss 状态、取消、失败重试和 Ray Job 日志。
6. 完成 `01-our-work.md` 中的代码安全改造后，再开放生产任务。

参考资料： [KubeRay API](https://ray-project.github.io/kuberay/reference/api/)，[Ray runtime environments](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html)。
