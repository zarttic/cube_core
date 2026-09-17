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

异构 worker 组存在时，实体剖分任务要钉到大内存组，需额外设置：

```text
CUBE_ENTITY_NODE_RESOURCE=cube_partition_worker_large
```

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

### 运行实例校验与修复

Worker 组一旦缺少 `resources` 声明，请求该逻辑资源的 task 会变成不可满足（infeasible）：
Ray 不报错也不超时，task 永久 pending，`ray_job` 任务一直处于运行中。日志特征是
`(raylet) There are tasks with infeasible resource requests ...` 和每 5 秒重复的
`(autoscaler) No available node types can fulfill resource requests
{'CPU': 1.0, 'cube_partition_worker': 1.0}*N`；此时扩容 worker 也不会让任务完成。
2026-09-17 的运行实例即因此导致容器数量大于 0 的剖分任务全部被人工取消。

只读校验（第二处的输出必须出现 `cube_partition_worker`）：

```bash
kubectl -n kuberay-system get raycluster cube-partition \
  -o jsonpath='{.spec.workerGroupSpecs[0].rayStartParams}{"\n"}'
kubectl -n kuberay-system exec <worker-pod> -- bash -lc 'RAY_ADDRESS=auto ray status' | grep cube_partition_worker
```

缺失时补回声明（`value` 必须是双重编码字符串，KubeRay 会把它渲染成
`--resources="{\"cube_partition_worker\": 1}"`）：

```bash
kubectl -n kuberay-system patch raycluster cube-partition --type=json \
  --patch '[{"op":"add","path":"/spec/workerGroupSpecs/0/rayStartParams/resources","value":"\"{\\\"cube_partition_worker\\\": 1}\""}]'
```

补丁对**新创建**的 Worker Pod 生效；改动后确认 `ray status` 出现
`cube_partition_worker`，再用一个容器数量大于 0 的小规模真实任务端到端确认。

### 异构 worker 组（大/小两组）

实体剖分（`isea4h`）的单个格元可覆盖整景，按格元窗口全分辨率读取时峰值内存实测可达 **1.6 GB**
（单景 34,025×15,457 px、1 波段 uint16），2026-09-17 曾因 2Gi Pod 上限被 Ray 内存监控杀掉 3 次；
而逻辑剖分（`geohash`/`mgrs`）的 chunk 任务普遍很小。因此生产集群用两组 worker：

| 组 | 容器 requests / limits | `num-cpus` | 声明资源 | `maxReplicas` | 用途 |
| --- | --- | --- | --- | --- | --- |
| `partition-workers` | 1 CPU / 2Gi（limits：2 CPU / 10Gi 临时盘） | 1 | `cube_partition_worker: 1` | 14 | `geohash`/`mgrs` 及其余任务 |
| `partition-workers-large` | 1 CPU / 4Gi（limits：2 CPU / 12Gi 临时盘） | 1 | `cube_partition_worker: 1` + `cube_partition_worker_large: 1` | 5 | `isea4h` 实体剖分 |

- 两组都用 `num-cpus: 1` 且各声明 1 个逻辑槽位，所以“一个 Worker Pod 同时只跑一个剖分任务”
  的语义不变；`limits.cpu` 给到 2 只是让单任务在 cgroup 层能突发多核，不改变调度语义。
- `isea4h` 任务通过 `CUBE_ENTITY_NODE_RESOURCE=cube_partition_worker_large` 钉到 4Gi 组
  （任务请求 `cube_partition_worker_large: 0.001`，只有该组声明了这个标签）。未设置该变量时
  实体任务与逻辑任务一样落在默认组，行为向后兼容。
- **每个组都必须声明 `cube_partition_worker: 1`**，否则 `worker_container_limit > 0` 的任务在该组上
  是不可调度请求，会永久 pending（见上一节的失败模式）；两边都声明才能保证 `limit=N` 跨组合计生效。
- 容量：可调度节点为 `poufennode02`/`poufennode03`/`gmhnode02`（`10.3.100.184`；`poufennode01` 已 cordon、`poufennode04` 不在集群），
  每节点可分配 16 CPU / `31564756Ki`（约 30.1Gi）；head 请求 2 CPU / 8Gi。worker 组请求合计
  14（小）× 2Gi + 5（大）× 4Gi ≈ 48Gi，在扣除 head 后的三节点理论余量内，因此
  `maxReplicas` 取 14（小）+ 5（大）（2026-09-18 集群只读核对）。照搬 36 会得到长期 Pending 的 Pod。

实体任务的读取现在是**分块流式**的：窗口像素不超过 `CUBE_ENTITY_READ_BLOCK_PIXELS`（默认
4,000,000 px ≈ 8 MB/int16 波段）时仍走原来的一次性 `rasterio.mask.mask` 快路径；超过则逐块读取 +
逐块掩膜，写入 `CUBE_ENTITY_TILE_TMP_DIR`（默认 `$CUBE_SOURCE_CACHE_DIR/entity_tiles`）下的临时
GeoTIFF，再流式 `fput_object` 上传并删除临时文件。内存上界随块预算而不是格元大小增长，代价是墙钟时间。

同一景、同一 L1 格元（窗口 34,025×15,457 px）的实测对比：

| | 峰值内存 | 耗时 | 产物 |
| --- | --- | --- | --- |
| 一次性 mask 读取 | **1.62 GB**（2Gi Pod 上被 Ray 内存监控杀掉） | 31.6 s | 346.0 MB，34025×15457 |
| 分块流式（4 Mpx 预算） | **422 MB** | 22.9 s | **同尺寸、同字节数、逐像素完全一致（0 个不一致）** |

校验只读命令：

```bash
kubectl -n kuberay-system get raycluster cube-partition \
  -o jsonpath='{range .spec.workerGroupSpecs[*]}{.groupName}{" "}{.maxReplicas}{" "}{.rayStartParams.resources}{"\n"}{end}'
kubectl -n kuberay-system exec <worker-pod> -- bash -lc 'RAY_ADDRESS=auto ray status' | grep cube_partition_worker
# 任务级：实体（isea4h）任务的 required_resources 应同时含槽位与 large 标签
```

## 集群与镜像要求

- Head 与 Worker 使用同一兼容 Ray 版本的运行镜像，镜像应包含
  `cube_encoder`、`cube_split`、`cube_web` 和栅格、对象存储、OpenGauss 驱动依赖。
- Worker 允许 `minReplicas: 0`，并设置明确的 `maxReplicas`。容量、CPU、内存
  与临时盘按真实影像大小和并发度配置；不能依赖节点本地源码目录或镜像携带影像。
- 生产集群的 `autoscalerOptions.idleTimeoutSeconds` 为 600 秒，但两个 worker 组各自设置了
  `workerGroupSpecs[].idleTimeoutSeconds: 1800`（组级值优先），因此空闲 Worker 实际按 1800 秒回收。
  归档模板只在 `autoscalerOptions` 上设置 600 秒，组级值由运行实例单独维护。
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
