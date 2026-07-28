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
CUBE_WEB_RAY_JOB_ADDRESS=http://<kubernetes-node>:<dashboard-nodeport>
CUBE_WEB_RAY_ADDRESS=auto
```

`CUBE_WEB_RAY_JOB_ADDRESS` 是 Web 调用 Ray Jobs API 的地址；`auto` 仅供
Ray Job driver 在集群内部连接 Head。OpenGauss、MinIO 与 Ray 的地址和凭据
同样只从运行时环境读取。凭据不可写入任务业务 payload、业务配置表或仓库。

当 Web 服务位于 Kubernetes 集群外时，地址必须是 KubeRay Dashboard 的 NodePort
或受控 Ingress，例如当前集群的 `http://<node-ip>:30265`。不能复用裸机 Ray
Dashboard 的 `:8265` 地址，否则 Job 会提交到另一套 Ray 集群。集群内服务可使用
`test-cluster-head-svc.default.svc.cluster.local:8265`。

## 集群与镜像要求

- Head 与 Worker 使用同一兼容 Ray 版本的运行镜像，镜像应包含
  `cube_encoder`、`cube_split`、`cube_web` 和栅格、对象存储、OpenGauss 驱动依赖。
- 镜像应来自所有节点可拉取的镜像仓库。若临时使用 `localhost/...` 镜像，必须先在
  每个可调度节点导入同一镜像；否则扩容会出现 `ImagePullBackOff`。
- Worker 允许 `minReplicas: 0`，并设置明确的 `maxReplicas`。容量、CPU、内存
  与临时盘按真实影像大小和并发度配置；不能依赖节点本地源码目录或镜像携带影像。
- 源影像和成果均使用 MinIO `s3://` URI。Worker 在自己的缓存目录下载源对象、
  校验哈希、处理后把成果写回 MinIO。
- Pod 必须能访问 MinIO、OpenGauss 与 Head Service。OpenGauss 的访问控制必须
  允许 Kubernetes Pod CIDR，而不是只允许节点网段。

### Worker 容量基线

当前 KubeRay 剖分集群的 Worker 配置为 `minReplicas: 0`、`maxReplicas: 30`。
每个 Worker 必须同时声明以下一致的资源值：

- Ray `rayStartParams.num-cpus: "1"`
- Kubernetes `requests.cpu` 与 `limits.cpu`: `1`
- Kubernetes `requests.memory` 与 `limits.memory`: `2Gi`

因此满扩容时 Ray 最多可调度 30 个 CPU slot，Kubernetes 的 Worker 容量上限为
30 CPU 和 60Gi 内存。更新容量时必须同时修改 Ray CPU 声明、Kubernetes request/limit
和 `maxReplicas`；只改其中一项会使 Ray 调度容量与 Pod 实际可用资源不一致。

`spec.enableInTreeAutoscaling` 必须开启，并显式设置
`spec.autoscalerOptions.version: v2`。Head 与 sidecar 的 Autoscaler 版本必须一致；
验证时应提交至少两个 `num_cpus=1` 的长任务，观察 `ray status` 的 pending demand
和 Worker Pod 数量一起增长。

## 验收顺序

1. 验证 Head 与 Worker Pod 能导入项目依赖，并连接 MinIO 和 OpenGauss。
2. 先对 MinIO 中真实源对象执行 `stat_object`，再提交一个小规模真实任务。
3. 验证 OpenGauss 输出版本、MinIO 输出对象和任务状态一致。
4. 逐级提高并发，观察 Worker 从 `0` 扩到上限后是否在空闲超时后回缩到 `0`。
5. 受控验证取消、失败重试和质量门禁；质量完成不得自动触发入库。

真实验收只使用独立命名空间的批次标识。运行完成后可关闭临时 API 或测试作业，
但不得删除需要保留的业务输出。
