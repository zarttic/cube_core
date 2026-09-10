# KubeRay 运行说明

## 范围

生产剖分由 `cube_web` 持久化任务，再通过 Ray Jobs API 提交到长期运行的
KubeRay `RayCluster`。每次业务批次不初始化一套新集群；Ray 自行调度批次内
独立单元，Autoscaler 按资源请求扩缩 Worker。

`geohash` 和 `mgrs` 仍是逻辑剖分，`isea4h` 是实体剖分。KubeRay 只替换
计算资源的调度方式，不改变这三个生产格网的业务契约。

## 运行配置

Web 服务必须通过运行时环境变量配置 Ray Jobs API，而不能写入
`cube_web_configs`：

```text
CUBE_WEB_PARTITION_EXECUTOR=ray_job
CUBE_WEB_RAY_JOB_ADDRESS=http://<ray-head-service>:8265
CUBE_WEB_RAY_ADDRESS=auto
```

`CUBE_WEB_RAY_JOB_ADDRESS` 是 Web 调用 Ray Jobs API 的地址；`auto` 仅供
Ray Job driver 在集群内部连接 Head。OpenGauss、MinIO 与 Ray 的地址和凭据
同样只从运行时环境读取。凭据不可写入任务业务 payload、业务配置表或仓库。

## 单任务容器数量限制

前端“容器数量”对应请求字段 `worker_container_limit`，表示一次剖分任务最多
占用的 KubeRay Worker Pod 逻辑槽位：

- `0` 表示不设置单任务上限，沿用系统默认并发，兼容历史请求；
- 大于 `0` 时，每个剖分 Ray task 请求一个 `cube_partition_worker` 逻辑资源，
  同一任务的 driver 同时最多保持该数量的活动 task；
- 该限制属于共享 RayCluster 中的任务级上限，不会为该任务独占一套 RayCluster。
  多个任务仍可共享集群容量，所有任务合计容量受 `maxReplicas`、CPU、内存和
  Kubernetes 配额约束；
- 逻辑剖分限制规划 task 的活动数量，实体剖分同时限制实体 task 的 fan-out，
  碳卫星任务当前为一个 Worker task。

Worker Pod 必须在启动时声明一个逻辑槽位；模板
`archive/docs/kuberay-migration/templates/raycluster.yaml`（迁移包，已归档） 已配置：

```yaml
rayStartParams:
  resources: '"{\"cube_partition_worker\": 1}"'
```

如果平台方修改资源名，Web 运行时的 `CUBE_WEB_RAY_WORKER_RESOURCE` 必须使用
同一个名称。`minReplicas` 和 `maxReplicas` 由平台方按集群容量设置；任务提交后
Ray 会因待调度的逻辑资源请求触发 KubeRay 扩容，空闲超时后回缩到配置的
`minReplicas`。该资源是 Ray 的逻辑调度资源，实际 Pod 的 CPU、内存仍由
Kubernetes requests/limits 控制。

## 集群与镜像要求

- Head 与 Worker 使用同一兼容 Ray 版本的运行镜像，镜像应包含
  `cube_encoder`、`cube_split`、`cube_web` 和栅格、对象存储、OpenGauss 驱动依赖。
- Worker 允许 `minReplicas: 0`，并设置明确的 `maxReplicas`。容量、CPU、内存
  与临时盘按真实影像大小和并发度配置；不能依赖节点本地源码目录或镜像携带影像。
- 生产集群的空闲 Worker 回收时间为 600 秒（`autoscalerOptions.idleTimeoutSeconds: 600`）。
  这个参数由 RayCluster 运行时配置控制，不写入业务配置表。
- 源影像和成果均使用 MinIO `s3://` URI。Worker 在自己的缓存目录下载源对象、
  校验哈希、处理后把成果写回 MinIO。
- Pod 必须能访问 MinIO、OpenGauss 与 Head Service。OpenGauss 的访问控制必须
  允许 Kubernetes Pod CIDR，而不是只允许节点网段。

## 验收顺序

1. 验证 Head 与 Worker Pod 能导入项目依赖，并连接 MinIO 和 OpenGauss。
2. 先对 MinIO 中真实源对象执行 `stat_object`，再提交一个小规模真实任务。
3. 验证 OpenGauss 输出版本、MinIO 输出对象和任务状态一致。
4. 逐级提高并发，观察 Worker 从 `0` 扩到上限后是否在空闲超时后回缩到 `0`。
5. 受控验证取消、失败重试和质量门禁；质量完成不得自动触发入库。

真实验收只使用独立命名空间的批次标识。运行完成后可关闭临时 API 或测试作业，
但不得删除需要保留的业务输出。
