# 业务方交接单

请业务方提供或确认以下事项。

## Kubernetes 与 KubeRay

- [ ] Kubernetes 集群、namespace 和 KubeRay Operator 版本。
- [ ] `RayCluster`、Pod、Service、Job、ConfigMap、Secret、PVC 的创建和查看权限，或对应 GitOps 提交流程。
- [ ] KubeRay CRD 版本和准入/镜像扫描要求。
- [ ] Ray Head Service 的集群内 DNS 名称，以及 Web 服务如何访问 Jobs API `:8265`。
- [ ] 日志、指标和告警的采集标准；Ray Job 日志的保留期限。

## 镜像和调度

- [ ] 私有镜像仓库地址、推送权限和 `imagePullSecret`。
- [ ] namespace CPU、内存、GPU 和临时存储配额。
- [ ] 可用节点标签、污点、容忍、GPU resource name，以及必须使用的 nodeSelector 或 affinity。
- [ ] 是否允许 Ray autoscaler；若允许，Worker 最小/最大副本范围。

## 网络、存储和密钥

- [ ] 从 Head/Worker Pod 到 MinIO、OpenGauss、镜像仓库和 DNS 的网络放行。
- [ ] MinIO endpoint、bucket、读写前缀和最小权限策略。
- [ ] OpenGauss PostgreSQL 兼容 DSN 的 Secret 注入方案及最小数据库权限。
- [ ] Kubernetes Secret 名称和键名约定。建议包含：
  `CUBE_WEB_POSTGRES_DSN`、`CUBE_WEB_MINIO_ENDPOINT`、`CUBE_WEB_MINIO_ACCESS_KEY`、`CUBE_WEB_MINIO_SECRET_KEY`、`CUBE_WEB_MINIO_BUCKET`。
- [ ] Worker 临时缓存使用的 `emptyDir` 或本地盘容量；建议单 Worker 至少按单景源文件和 COG 转换峰值预留。

## 业务方不需要做的事

- 不需要在每台节点安装项目源码或手工加入 Ray Worker。
- 不需要保存 Cube 项目的密钥到业务配置表。
- 不应给任务参数传递 MinIO 密钥或 OpenGauss DSN。

## 需共同确认的决策

1. `cube_web` 是否部署在同一 Kubernetes 集群。推荐是，以便通过 Head Service 的集群内 DNS 调用 Jobs API。
2. 是否共享长期 RayCluster。推荐是，当前 Web 的任务状态和取消逻辑与此模式匹配。
3. Head、Worker 与 Web 是否使用同一个运行镜像。推荐 Head/Worker 使用同一 Cube 计算镜像；Web 可用同镜像或专用 Web 镜像。
4. 是否需要 GPU。当前剖分链路默认 CPU；如需 GPU，业务方必须给出节点池和 Kubernetes 资源名。
