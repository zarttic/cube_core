# 单景剖分性能结论与优化方法（2026-06-20）

## 范围

- 验证入口：`POST /v1/partition/{data_type}/tasks/run`
- 当前分支：`fix/partition-remote-job-runtime`
- 当前系统源数据：光学、产品、碳遥感均使用当前系统样本；雷达使用当前系统 `cube/source/radar/` 前缀检查

## 结果

| 案例 | 任务 | 结果 | 分区耗时 | 总耗时 | 10 秒目标 |
|---|---|---:|---:|---:|---|
| optical 单景逻辑剖分（当前） | `partition-2a3d3d6b792b` | 12 rows | 9.733s | 9.865s | 达成 |
| optical 单景逻辑剖分（补丁前） | `partition-cff261ae20c4` | 12 rows | 10.653s | 11.032s | 未达成 |
| optical 单景实体剖分 | `partition-695370bb2879` | 6224 rows | 67.006s | 68.149s | 未达成 |
| product 单景 | `partition-055850436986` | 4 rows | 10.320s | 10.401s | 接近，未达成 |
| carbon 单文件（`nc4`，1000 observations） | `partition-d74ee6ceef20` | 1000 rows | 27.280s | 27.280s | 未达成 |
| radar 单景 | `partition-18b71aff1ec7` | 阻塞 | - | - | 无法评估 |

补充：

- 当前 MinIO `cube/source/radar/` 前缀共 48 个对象，`.hdr` 数量为 0，`.tif/.tiff` 数量为 0。
- product 补丁前的正式单景任务 `partition-5d46492f9d6c` 失败，补丁后恢复为成功。

## 今天已验证有效的改动

1. Web 远端 Ray Job 运行时修复：
   - 移除 worker 对 `fastapi.HTTPException` 的硬依赖
   - 让远端 job 带上 `.cube_web.env` 解析后的 Ray / MinIO / PostgreSQL 运行时值
   - 避免外层 Ray Job runtime env 与内层 `ray.init(runtime_env=...)` 冲突

2. Ray worker 本地 COG 优化：
   - 逻辑剖分和产品剖分 worker 先把 source 转为 worker 本地 COG
   - 直接用本地 COG 生成 index rows
   - 只在 rows 生成后上传一次 COG，并把结果中的 `asset_path` 回写为共享的远端 URI

这个改动已经带来两个直接结果：

- optical 单景逻辑剖分从 `10.653s` 降到 `9.733s`
- product 单景从“正式链路失败”恢复到 `10.320s` 成功完成

## 当前瓶颈判断

1. 单资产任务分组仍按 `asset_path` 聚合。
   - `optical s2` 和 `product s2` 的正式单景任务都只有 1 个 Ray worker 真正参与
   - 当前 `ray_parallelism` 虽然可配置，但单资产场景下仍然被任务分组限制住

2. `product` 已不再卡在重复远端读写上，但仍然受单资产串行分组影响。

3. `carbon` 的 10 秒目标不现实，至少对当前 `1000 observations / nc4` 样本不是。
   - 该路径的主要成本是 observation 读取与 point-to-cell 编码

4. `radar` 当前不是性能问题，而是源数据可读性前置条件不满足。

## 项目级优化方法

### 第一优先级：单资产内部分块并行

在 `cube_split.jobs.ray_partition_core._group_tasks_for_local_processing()` 之后增加单资产切块：

- 当同一 `asset_path` 对应的 cell 数超过阈值时，不再只生成 1 个 group
- 改成在同一 worker 本地 COG 路径上，按 cell 数或 window 数切成多个 group
- 保持“每个 group 只处理本地 COG，不重复上传、不重复远端下载”

预期收益：

- optical 单景会有更稳定的 10 秒以内余量
- product 单景最有机会从 `10.320s` 压到 10 秒内

### 第二优先级：缓存生命周期治理

- 对 `/tmp/cube_split_source_cache` 增加按大小或最近访问时间清理
- 对 `/tmp/cube_web_partition_run` 增加压测后清理约定
- 避免重复压测时再次触发 `No space left on device`

### 第三优先级：carbon 输入打包路径标准化

- 允许正式远端 job 以 repo 相对路径或 MinIO staging 目录引用 `nc4`
- 避免只能依赖本机绝对路径，减少“本地可见、远端不可见”的输入路径问题

### 第四优先级：radar 提交前预检

- 在 Web 提交前预检 `.dat` 是否存在同名 `.hdr`，或是否已提供可读的 `.tif/.tiff`
- 不满足条件时在提交前直接返回明确错误，避免无效分区任务占用 Ray 资源

## 结论

- 当前项目已经证明：**optical 单景逻辑剖分可以在正式 Web 链路下跑进 10 秒**。
- `product` 单景已经恢复成功，但距离 10 秒还差约 `0.32s`。
- `carbon` 当前样本形态下不满足 10 秒目标。
- `radar` 当前阻塞在源数据可读性，不应继续拿性能优化掩盖数据前置问题。

下一步最值得做的不是继续堆 Ray 并发参数，而是实现“**单资产内部分块并行 + 保持 worker 本地 COG 处理**”。
