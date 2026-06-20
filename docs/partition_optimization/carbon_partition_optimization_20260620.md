# 碳卫星剖分优化记录（2026-06-20）

## 1. 范围

本次优化针对碳卫星 `xco2` 剖分链路，覆盖两类输入：

- OCO-2 Lite `.nc4`
- TanSat 风格 `.h5/.hdf`

目标是解决单景碳卫星任务在 Ray 集群上的读取和剖分效率问题，并让任务真正利用多节点并行，而不是只在 driver 端完成大部分工作。

## 2. 优化前是什么样子

优化前的正式链路主要有两个问题：

1. `nc4/h5` 文件先在 driver 进程整文件读取。
   - driver 先把整景文件解析成 `CarbonSatelliteObservation` 列表
   - 再切 chunk 分发给 Ray worker
   - 也就是说，Ray 只并行了“点转格网编码”这一步，**没有并行读取源文件**

2. OCO-2 的 `netCDF4` 读取路径不稳定。
   - 代码会把 `numpy.ndarray size changed` 这种运行时告警误判成 `netCDF4` 不可用
   - 结果退回到 `h5dump` 命令行读取
   - 这会把读取阶段放大成主要瓶颈

这条老路径就是“旧的 driver 预读路径”。它本质上和“本地文件输入模式”是绑定的：  
先在 driver 可见的本地路径上把文件读完，再把 Python 对象发给 Ray。

## 3. 优化前性能

### 3.1 读取阶段基线

对同一个 OCO-2 `nc4` 样本：

- `1000 observations` 旧读取耗时：约 `24.5s ~ 24.9s`

这个阶段还没进入真正的格网剖分，时间基本都花在了错误退回 `h5dump` 的读取上。

### 3.2 正式剖分链路基线

| 场景 | 配置 | 优化前耗时 |
|---|---|---:|
| 单景 `1000 observations` | Ray 4 workers | `27.280s` |
| 单景全量 `145,711 observations` | Ray 8 workers | `22.125s` |

说明：

- `1000 observations / 27.280s` 来自正式碳卫星剖分任务基线
- `145,711 observations / 22.125s` 是修复 `netCDF4` 误回退之后、但仍然走“driver 预读”链路时的端到端结果

也就是说，哪怕 `netCDF4` 已经恢复，**只要读取仍然集中在 driver 端，整条链路还是会被单机预读拖住**。

## 4. 做了什么优化

### 4.1 修复 OCO-2 读取路径

只在 `netCDF4` 模块真实缺失时，才回退到 `h5dump`。

效果：

- `1000 observations` 读取耗时从 `24.5s ~ 24.9s` 降到 `0.18s ~ 0.19s`

### 4.2 把 Ray runtime env 收瘦并稳定下来

处理了两类问题：

1. runtime env 打包过大
   - 排除了 `.codex`、`.codegraph`、`.cache`、`.venv`、`.tmp` 等本地垃圾目录

2. `10.3.100.180` 节点 `/tmp` 被旧缓存打满
   - `180` 的 `/tmp` 是单独的 `16G tmpfs`
   - 清理旧缓存后，节点恢复可用
   - 同时加入 `CUBE_CARBON_RUNTIME_ENV_REV=20260620`，绕开旧失败缓存

另加了一层兜底：

- 如果某次 worker 因 `RuntimeEnvSetupError: No space left on device` 失败，会临时退到 Ray head 节点继续执行，避免整任务失败

### 4.3 把读取并行化前移到 worker

这是本次最核心的优化。

新链路改成：

1. driver 不再整文件读取 `nc4/h5`
2. driver 只根据 `source_uri` 和记录总数做区间切片
3. Ray worker 收到的是：
   - `source_uri`
   - `start_index`
   - `stop_index`
4. worker 自己从 MinIO 拉源文件到本地缓存
5. worker 只读取自己负责的 slice
6. worker 在本地完成 slice 对应的剖分，再回传结果

也就是说，现在并行化覆盖了两步：

- 源文件读取
- 格网剖分

### 4.4 为 worker 补齐 MinIO 运行时信息

为了让 worker 能直接从 `s3://...` 拉源文件，runtime env 里补齐了：

- `CUBE_WEB_MINIO_ENDPOINT`
- `CUBE_WEB_MINIO_ACCESS_KEY`
- `CUBE_WEB_MINIO_SECRET_KEY`
- `CUBE_WEB_MINIO_BUCKET`

### 4.5 修复标准碳卫星 schema 的源文件 URL

标准载入 schema 里的碳卫星 `source_uri` 原来文件名写错，少了 `(1)`：

- 错误：`...220729012824s.nc4`
- 正确：`...220729012824s(1).nc4`

这个问题不修，Web 从标准 schema 直接跑 MinIO 正式任务时会找不到对象。

### 4.6 增加 TanSat `.h5` 支持

当前碳卫星读取层已经分成两条：

1. OCO-2 专用快路径
2. 通用 netCDF/HDF `xco2` 路径

通用路径按字段映射读取，优先识别：

- `latitude/lat`
- `longitude/lon`
- `time`
- `xco2`
- `xco2_quality_flag / quality_flag / retr_flag / qa_value`
- `exposure_id / exposureID / observation_id`

因此当前已经支持 TanSat 风格 `.h5/.hdf` 输入进入同一条 worker 切片剖分链路。

## 5. 优化后性能

### 5.1 `1000 observations`

| 场景 | 配置 | 优化前 | 优化后 |
|---|---|---:|---:|
| 单景 `1000 observations` | Ray 4 workers | `27.280s` | `2.006s` |

结果：

- 提升约 `13.6x`

### 5.2 `20,000 observations`

| 场景 | 配置 | 优化后耗时 |
|---|---|---:|
| 单景 `20,000 observations` | Ray 8 workers | `3.892s` |
| 单景 `20,000 observations` | Ray 16 workers | `3.255s` |

### 5.3 单景全量 `145,711 observations`

| 场景 | 配置 | 耗时 |
|---|---|---:|
| 旧链路：driver 预读 + Ray 剖分 | Ray 8 workers | `22.125s` |
| 新链路：worker 切片读取 + Ray 剖分 | Ray 8 workers | `13.230s` |
| 新链路：worker 切片读取 + Ray 剖分 | Ray 16 workers | `8.806s` |

结果：

- `Ray 8 workers` 相比旧链路下降约 `40.2%`
- `Ray 16 workers` 相比旧链路下降约 `60.2%`

## 6. 集群并行情况

`Ray 16 workers` 的落点探针显示，当前任务已经能稳定分散到 4 个节点：

- `10.3.100.179`: 3
- `10.3.100.180`: 3
- `10.3.100.181`: 5
- `10.3.100.182`: 5

说明：

- `180` 节点现在不会被自动绕过
- 碳卫星任务已经不再是“driver 单机读完、其他节点只做少量计算”的模式

## 7. 当前状态

目前碳卫星剖分优化已经完成以下落地：

- OCO-2 `.nc4` 支持 worker 侧切片读取并剖分
- TanSat 风格 `.h5/.hdf` 支持进入同一条 worker 侧切片读取链路
- `source_uri` 直接来自 MinIO 元数据，不再要求 driver 先把整文件落到本地
- Ray 8 / 16 workers 都已完成真实 MinIO 源文件验证

## 8. 当前边界

还保留旧的 driver 预读回退路径，主要用于这些场景：

- `jsonl/csv` 输入
- `selected_source_indexes` 这种稀疏选点模式
- 不满足当前 worker 切片条件的输入

另外还有一个现实边界：

- 当前仓库和 MinIO 中没有真实 TanSat `.h5` 样本
- 已完成的是“按 TanSat 常见字段组织方式”的兼容实现和单测
- 如果后续接入的真实 TanSat 文件字段名不同，还需要拿一份真实样本再做一次字段映射校正

## 9. 结论

本次碳卫星剖分优化的关键，不是单纯增加 Ray worker 数量，而是把并行化前移到了**源文件读取阶段**。

优化前：

- driver 先整文件读取
- Ray 只并行后半段剖分

优化后：

- worker 直接从 MinIO 拉源文件
- worker 按 slice 读取
- worker 本地完成剖分

因此这次优化真正解决的是“单景碳卫星任务并行开始得太晚”的问题。当前结果表明，这条链路已经从 `1000 observations / 27.280s` 压到 `2.006s`，并把全量 `145,711 observations` 压到 `Ray 16 workers / 8.806s`。
