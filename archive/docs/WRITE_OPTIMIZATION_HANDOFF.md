# 写入路径优化与真实剖分入库测试 Handoff

> 状态更新：2026-09-12 傍晚（agent 续作完成）。目标（用户 goal）：实现推荐的写入优化，并**必须做真实剖分入库测试**（KubeRay 预热时间不算入库时间）。
> 当前停在：代码优化已提交 `69bfb52`；**真实剖分 + 真实入库验收已执行并通过**（`status=passed`，前缀 `real-accept-0d411c506d50`，证据见 §九）。
> 剩余：Linear ZAR-95 的 comment 未写入（本次会话没有 Linear MCP 工具，见 §九.8）；原待办 4（`product_ingest_job`）已给出带证据的决策（§九.7）。

## 一、当前状态总览

| 事项 | 状态 |
|---|---|
| 代码优化（5 文件） | ✅ 已完成，跨包 pytest **947 passed**，已提交 `69bfb52` |
| 代码提交 | ✅ `69bfb52 perf: switch partition/ingest writes from batch MERGE to COPY and set-based INSERT`（6 文件） |
| OpenGauss 全局参数调优 | ✅ 已生效（主备两节点，7 项） |
| DB 维护（TRUNCATE/VACUUM） | ✅ 已完成（staging 3.59GB→0，库 7.9→4.32GB） |
| **真实剖分测试** | ✅ 已执行并通过：6 个 partition run（geohash/mgrs/isea4h + cancel/quality 探针）全部 completed；`driver.bootstrap` 0.122–0.358s（见 §九.3） |
| **真实入库测试** | ✅ 已执行并通过：4 个真实数据集手动托管入库 completed，RS 表实际行数与 `rs_ingest_job.stats_json` 一致（见 §九.4） |
| 验收脚本终态 | ✅ `cube_web/scripts/run_real_partition_acceptance.py` 返回 **`status=passed`** |
| Linear 事项 | ⏳ **ZAR-95** 已建并 In Progress；comment 待写入（本会话无 Linear MCP 工具） |

## 二、已完成的代码改动（已提交 `69bfb52`）

全部通过对应单测（57 passed：test_ray_ingest_job / test_managed_output_ingest / test_product_workflow / test_partition_domain_store；此前 cube_web 域存储相关 107 passed）。

### 1. `cube_web/cube_web/services/partition_domain_store.py`

- **`_merge_insert_many` → `_copy_insert_many`**：批量写入改为「CTAS 建临时表（类型取自目标表）+ COPY 写入 + 单条 `INSERT ... SELECT ... WHERE NOT EXISTS`」。`complete_output` 的批量阈值 500→5000。
- **`promote_logical_staging`**：3 个逐批 JSONB `MERGE` 改为 3 个集合式
  `INSERT ... SELECT DISTINCT ON (output_id) ... FROM staging WHERE NOT EXISTS(...) ORDER BY output_id, chunk_id, row_number`（保持原幂等/去重语义）。
- **`ensure_schema`**：加 `_domain_schema_current` 版本预检 + 实例级 `_schema_applied` 缓存，schema 已是最新时不再跑全量 DDL。

### 2. `cube_web/cube_web/services/partition_job_store.py`

- `PostgresPartitionJobStore.ensure_schema` 增加 `_schema_ready` 预检（9 个列标记 + 4 个索引标记 + domain schema 版本，单条 SELECT），已就绪则跳过 9×ALTER + 4×CREATE INDEX + apply_schema——针对历史上 `driver.bootstrap` 102.8s（DDL 锁等待）的修复。

### 3. `cube_split/cube_split/ingest/ray_ingest_job.py`

- `upsert_raw_assets_postgres` / `upsert_cube_facts_postgres`：改为「COPY 到类型化临时表（`_copy_rows_to_temp`）→ 单条 `MERGE FROM (SELECT DISTINCT ON (keys) * FROM temp)`」，替代逐 1000 行 VALUES MERGE。
- 新增辅助：`_copy_rows_to_temp` / `_deduplicated_temp_source` / `_drop_temp_table`；`_batches`/`_flatten`/`_postgres_values_source` 保留（product_ingest_job 仍在用）。

### 4. 测试同步更新

- `cube_web/tests/test_partition_domain_store.py`：monkeypatch 目标改名 `_copy_insert_many`。
- `cube_split/tests/test_ray_ingest_job.py`：`test_postgres_upserts_copy_rows_then_merge_once`（FakeCursor 增加 `copy()`，断言 1×CREATE TEMP + 1×COPY + 1×MERGE + 1×DROP、行数、ST_SetSRID、DISTINCT ON）。

## 三、OpenGauss 端已生效的调优（不依赖代码）

主备两节点（Primary 10.3.100.180 / Standby 10.3.100.181；.179/.182 不跑 DB）通过 `gs_guc reload -D /data/og_user/openGauss/install/data/dn -c 'k=v'` 写入：

| 参数 | 原值 | 现值 |
|---|---|---|
| checkpoint_completion_target | 0.5 | 0.9 |
| autovacuum_naptime | 600s | 120s |
| autovacuum_vacuum_scale_factor | 0.2 | 0.1 |
| autovacuum_analyze_scale_factor | 0.1 | 0.05 |
| bgwriter_delay | 2000ms | 200ms |
| log_min_duration_statement | 1800000ms | 5000ms |
| track_io_timing | off | on |

维护：TRUNCATE staging、11 表 VACUUM ANALYZE。回滚 = 同命令写回原值。

## 四、待办清单（按优先级）

1. ~~提交代码~~ ✅ 已提交 `69bfb52`；Linear 事项 ZAR-95 已建。
2. ~~真实剖分测试~~ ✅ 已执行（见 §九.2、§九.3）。入口是验收脚本本身：`POST /v1/partition/schemas/import` → `POST /v1/partition/runs`（3 个格网）+ cancel/quality 探针；全部经 Ray Job（`http://10.3.100.183:30826`）执行，`ray.wait` 与 Ray Job Server 排队时长单独记录、不计入 DB 写入耗时。
3. ~~真实入库测试~~ ✅ 已执行（见 §九.4）。路径为 `cube_web/services/ingest_worker` → `cube_split.ingest.managed_output_ingest.ingest_managed_output` → `ray_ingest_job` 的「COPY 到类型化临时表 + 单条 MERGE」；记录了行数与耗时。
4. **`product_ingest_job` 决策：本轮不改**（见 §九.7）。本轮 product 数据集走的是 isea4h 实体输出 → COPY 路径；逐批 VALUES MERGE 只在 product + logical 组合生效，需要专门场景才能量到收益，且按实测它不是当前瓶颈。
5. Linear：`ZAR-95` comment（验收前缀、attempt / Ray Job ID、timings、行数、验证命令、`status=passed`）待有 MCP 的会话补记。

## 五、Baseline（对比用）

**微基准（OpenGauss，临时表，20k 行，同构索引）**：
- 旧写法 MERGE 逐批：500 行/批 ≈ **14.9k rows/s**（batch 越大越慢，5000 行/批 ≈ 9.8k）
- 新写法 COPY 直写 ≈ **89.6k rows/s**；COPY→staging + INSERT SELECT ≈ **88.8k rows/s**
- 提交延迟 sync=on 1.5ms / off 0.62ms；8 线程并发提交 ~3.1k commits/s
- openGauss 7.0.0-RC3 不支持 `INSERT ... ON CONFLICT`、无 `lock_timeout`（只有 `lockwait_timeout`）

**历史真实运行分解（改前）**：
- `partition-d680d717da27`（逻辑 mgrs，40 chunk）：总 91.6s；ray.wait 88.1s；staging 1.64s；promote 2.08s
- `partition-990b7e15a1e3`：88.2s；ray.wait 86.5s；DB 阶段 1.4s
- `partition-02a29f0d59c1`（碳，ISEA4H）：132.0s；ray.wait_get 86.6s；**opengauss.complete_output 44.75s**
- `partition-bd81e80f4b60`：job_driver 126.7s；**driver.bootstrap 102.8s**；staging 8.94s；promote 7.31s
- 入库：carbon 70909 行 18.8s 总耗时；cube_fact 3283 行 3.8s

## 六、环境与访问注意事项

- openGauss 初始用户 `og_user` 禁止远程登录；管理路径：`ssh root@10.3.100.180` → `su - og_user` → `gsql -h /data/og_user/openGauss/tmp -p 15400 -d postgres`（socket，免密）。
- 凭据：用户提供的 root/DB 密码一律不入库、不写文件（对话中已明文出现，建议轮换）。
- KubeRay Ray Job Server：`http://10.3.100.183:30826`（api/jobs 可查作业状态与时长）。
- 工作区是共享的：提交时只 add 本任务文件；`git add -u cube_split cube_web` 前先 `git status` 核对。

## 七、下一 agent 继续执行

### 7.1 开始前检查

```bash
git status --short
git log --oneline -8
```

继续使用 Linear 事项 **ZAR-95**，不要新建重复事项。每个独立 commit 都要在事项中记录 hash、变更范围和验证结果；真实门禁全部通过后再将事项设为结束。

### 7.2 启动本地 Web API

本地 `.cube_web.env` 已存在但不能提交、打印或全量输出。需要把配置传给 shell 时，使用受控子 shell：

```bash
set -a
. "${CUBE_WEB_ENV_FILE:-$PWD/.cube_web.env}"
set +a
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 127.0.0.1 --port 50039
```

启动后先检查 `/health`。当前配置已核对为 Ray Job executor，KubeRay Job Server 为 `http://10.3.100.183:30826`，Ray 地址使用 `auto`。Job Server 会上传当前 worktree；执行真实任务前要确认没有未预期的代码改动。

鉴权默认开启。真实门禁必须传入受控的 Bearer Token；`auth_service.py` 使用配置的 HS256 签名校验，token 只放临时进程环境，不写 manifest、文件、命令输出或 handoff。`CUBE_WEB_AUTH_REQUIRED=0` 只可用于本地测试，不能用来宣称真实验收通过。

### 7.3 准备真实源 manifest

`cube_web/scripts/run_real_partition_acceptance.py` 的默认文件 `/tmp/cube-real-acceptance-prepared.json` 当前不存在。可以直接生成一个包含 `datasets` 的 manifest，绕过脚本默认只扫描 `cube/source/carbon/` 的自动发现逻辑。manifest 至少需要 4 个 scene，覆盖 `optical`、`radar`、`product`、`carbon` 四类；每个 asset 需要 `s3://bucket/key`、实际 64 位 SHA-256，以及适合该类型的 COG/GeoTIFF 或 NetCDF/HDF5 元数据。

已用 MinIO 对象清单和读取检查发现的候选对象如下。提交前仍要对所有对象重新 `stat_object`，checksum 必须由流式读取对象得到，不能凭 ETag 代替：

| 用途 | 候选对象 | 已知事实 |
|---|---|---|
| 光学小对象 | `s3://cube/cube/source/perf/dataset=perf_ray/sensor=optical_perf/acq_date=2020/07/01/scene_id=PERF_RAY_1/version=v1/optocal_readback_20200701_3584_b234.tif` | 约 13.9MB，MinIO stat 成功 |
| 光学合成对象 | `s3://cube/cube/source/synthetic/grid_adapted/gridadapt-20260705173838/geographic_optical_epsg4326_4096.tif` | 33.6MB，单波段 4096×4096，EPSG:4326，范围约 `[116,35,119,37]` |
| 雷达对象 | `s3://user-1/datas/2017-2021年哨兵一号合成孔径雷达北极海面风场数据产品/SSW_S1A_EW_GRDM_20170101T022434_8F4E_V1.1.tif` | 584,812 bytes，6 bands，EPSG:4326，范围约 `[63.70136,75.70482,87.34201,80.55616]` |
| 信息产品合成对象 | `s3://cube/cube/source/synthetic/grid_adapted/gridadapt-20260705173838/product_eco_security_epsg32648_4096.tif` | 约 67.1MB，EPSG:32648 |
| 信息产品真实源 | `s3://user-1/datas/1980-2020年滇中地区30米生态安全评价数据集（第一版）/1980-2020年滇中地区30米生态安全评价数据集（第一版）_2010年.tif` | 约 577.6MB；提交前必须重新 stat/hash |
| 碳卫星 XCO2 | `s3://user-1/datas/2017年3月至2018年5月全球TanSat XCO2数据集（第二版）-01/TanSat_ACGS_SCI_ND_L2_XCO2_lite_20170303.nc` | 约 1.64MB；提交前必须重新 stat/hash |

`cube/cube/source/carbon/` 当前递归对象数为 0，因此不要依赖 `discover_carbon_asset` 的默认扫描；对 `user-1` carbon URI 使用显式 manifest。数据库里已有部分 `user-1/cog/20260821_...` 的历史 URI，其中有对象已不存在，不能直接复制旧记录。若必须使用“真实业务源”而不是基础设施链路测试，优先使用 `user-1/datas` 对象；若先验证写入链路，合成对象更小，但结果中必须注明其性质。

生成 manifest 的临时脚本和 JSON 放在 `/tmp`，不要把凭据、DSN、绝对本地数据路径或大影像提交到仓库。Ray worker 不应读取 driver 的 `/tmp/.../cog/*.tif`；源对象必须使用 MinIO URI。

### 7.4 执行完整真实验收

脚本固定覆盖：

- `geohash` / logical / level 1；
- `mgrs` / logical / level 1；
- `isea4h` / entity / level 6；
- 重复提交幂等性、取消与 retry；
- quality pass/warn/fail；
- 手动托管入库；
- publication active/withdrawn；
- OpenGauss bridge/orphan 检查。

受控命令模板：

```bash
set -a
. "${CUBE_WEB_ENV_FILE:-$PWD/.cube_web.env}"
set +a
export CUBE_WEB_ACCEPTANCE_TOKEN="<临时 token，不要写入文件或输出>"
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 \
  cube_web/scripts/run_real_partition_acceptance.py \
  --manifest /tmp/<已验证-manifest>.json \
  --base-url http://127.0.0.1:50039
```

`--no-wait` 只能表示已提交，不能作为验收结论。成功条件是脚本最终返回 `status=passed`，三个 grid run、控制探针、quality/ingest、publication 和数据库检查都通过。脚本当前期望命名空间下至少 6 个 partition runs，具体以脚本版本中的 `EXPECTED_RUN_COUNT` 为准。

### 7.5 采集并回写结果

剖分完成后，查询 `partition_job_attempts.runner_result`，至少记录 `driver.bootstrap`、`opengauss.logical_stage`、`opengauss.promote_logical_staging`、`opengauss.complete_output`。入库完成后查询 `rs_ingest_job` 的 `started_at`、`finished_at`、`stats_json`，并核对 `ingest_runs`/`ingest_run_scenes` 完成计数。Ray Job 排队、`ray.wait` 和 worker 预热单独记录，不计入 OpenGauss 写入耗时。

将脱敏结果补回本文档，并在 ZAR-95 添加 comment，包含任务/attempt ID、timings、入库行数、验证命令和结果。不要写入 token、MinIO 凭据、DSN、完整 `.cube_web.env` 或含 secret 的 Ray 日志。若真实门禁暴露了 `product_ingest_job` 仍使用逐批 VALUES MERGE，再根据实测决定是否另做该路径优化。

## 八、排障与安全边界

- 生产入口仍只允许 `geohash`、`mgrs`、`isea4h`；不要新增 `s2`、`tile_matrix`、`plane_grid`。
- `geohash`/`mgrs` 固定 logical，`isea4h` 固定 entity；不要让前端混选。
- `max_cells_per_asset=0` 表示无上限。完整 ISEA4H level 6 可能产生大量 IO；level 1 只能作为另行记录的 smoke，不能替代完整门禁。
- `s3://` 质检不能用 `Path.exists()`；worker 侧下载到自己的缓存后再用 rasterio 打开。中文 key 解析后只 URL-decode 一次，缓存按 URI 稳定 hash 隔离并校验 SHA-256。
- 发生 `ENOSPC` 时只能清理该 worker 的 `/tmp/cube_split_source_cache` 后重试一次，不能清理整个 `/tmp`。
- Job Server 的历史 jobs API 可能带 runtime_env，禁止全文打印到终端或写入交接材料。
- 工作树中未提交的改动应只有本文件（验收结果回写），以及数据库里 `real-accept-0d411c506d50%` / `smoke-b2f5e0588245%` 前缀的测试记录；提交前只 add 本文件，避免把无关用户改动带入 commit。

## 九、真实验收结果（2026-09-12，agent 续作）

### 9.1 结论

- 验收脚本 `cube_web/scripts/run_real_partition_acceptance.py` 以真实 MinIO 对象运行，最终输出 **`status=passed`**。
- 验收命名空间：`real-accept-0d411c506d50`（token 由脚本生成），批次 `real-accept-0d411c506d50-batch`。
- 6 个 partition run（geohash/logical L1、mgrs/logical L1、isea4h/entity L6、cancel 探针、quality warn 探针、quality fail 探针）全部 completed；4 个数据集手动托管入库全部 completed；publication active→withdrawn；数据库结构检查满足 `datasets=7>=6`、`scenes=10>=7`、`partition_runs=6==EXPECTED_RUN_COUNT`、四类孤儿行全 0。
- 脚本自身计时：`partition_seconds=327.093`、`quality_ingest_seconds=15.559`、`quality_probes_seconds=21.94`、`publication_seconds=0.151`；整轮约 6.4 分钟（14:54:07→15:00:30）。

### 9.2 真实源 manifest

构建脚本 `/tmp/cube_accept/build_manifest.py`、JSON `/tmp/cube_accept/real-acceptance-manifest.json`（均在 /tmp，不提交）。每个对象先 `stat_object` 再做**流式 SHA-256**（对象上没有 `x-amz-meta-sha256` 元数据，因此没有用 ETag 代替 checksum）：

| 用途 | 对象键（bucket 前缀见下） | size (B) | sha256（前 12 位） | 关键事实 |
|---|---|---|---|---|
| optical 场景 1 | `cube/cube/source/perf/dataset=perf_ray/sensor=optical_perf/acq_date=2020/07/01/scene_id=PERF_RAY_1/version=v1/optocal_readback_20200701_3584_b234.tif` | 13,898,169 | `b272a59f977a` | 3 band(sr_band2/3/4), EPSG:4326, 0.00030906° |
| optical 场景 2 | 同上 `acq_date=2020/07/02/scene_id=PERF_RAY_2/...` | 13,898,169 | `b272a59f977a` | 与场景 1 内容相同、对象键不同 |
| radar 场景 | `user-1/datas/2017-2021年哨兵一号合成孔径雷达北极海面风场数据产品/SSW_S1A_EW_GRDM_20170101T022434_8F4E_V1.1.tif` | 584,812 | `d0c90f2268f3` | 6 band(WindSpeed/WindDirection/IncidenceAngle/Sigma0/Mask/qc_flag), EPSG:4326 |
| product 1980 | `user-1/datas/1980-2020年滇中地区30米生态安全评价数据集（第一版）/..._1980年.tif` | 594,085,235 | `0031bf4fe6e9` | float64, EPSG:32648, 30 m |
| product 2010 | 同上 `..._2010年.tif` | 577,612,716 | `d0d22f107838` | float64, EPSG:32648, 30 m |
| carbon 20170301 | `user-1/datas/2017年3月至2018年5月全球TanSat XCO2数据集（第二版）-01/TanSat_ACGS_SCI_ND_L2_XCO2_lite_20170301.nc` | 1,387,657 | `36f6419d3d43` | NetCDF4, 3699 观测, 含 latitude/longitude/time/xco2 |
| carbon 20170303 | 同上 `..._20170303.nc` | 1,644,457 | `6039de85b72d` | NetCDF4 |

manifest 结构：4 个 dataset / 7 个 scene，覆盖 `optical`、`radar`、`product`、`carbon`；radar 波段按文件真实语义声明为 `band_type=variable`（不是极化通道，见 §9.6）。`--prepare-only` 通过说明所有 s3 源对象存在且非空。

### 9.3 真实剖分：计时分解

来源：`partition_job_attempts.runner_result.timings`（`workflow_dataset.*` 为数据集级 DB 阶段，`ray_job_driver.*` 为 driver 级）。

| run（batch 后缀） | 格网/层级 | Ray Job 时长(s) | attempt 时长(s) | `opengauss.logical_stage` | `opengauss.promote_logical_staging` | `opengauss.complete_output` | `driver.bootstrap` | `ray.wait` |
|---|---|---|---|---|---|---|---|---|
| `-batch-geohash` (`partition-7102999ad725`) | geohash / logical L1 | 13.68 | 10.255 | 2.992s (n=58) | 0.286s (n=3) | 1.598s (n=4) | **0.122s** | 3.250s (n=59) |
| `-batch-mgrs` (`partition-9890fd667e48`) | mgrs / logical L1 | 137.13 | 133.261 | 28.125s (n=58) | **106.932s (n=3)** | 1.970s (n=4) | **0.126s** | 20.108s (n=62) |
| `-batch-isea4h` (`partition-871854359def`) | isea4h / entity L6 | 174.74 | 171.032 | worker 侧写入（无该阶段） | 不适用 | 6.227s (n=4) | **0.358s** | `raster_batch_driver.ray.wait` 2070.193s (n=154，累计) |
| `-cancel-batch-cancel-probe`（先取消 `partition-6df2d86dfa0b`，重试 `partition-3db390ac2d73`） | geohash L1 | 4.43 STOPPED + 8.66 | 4.595 | 1.321s (n=2) | 0.023s (n=1) | 0.013s (n=1) | **0.186s** | 3.828s (n=5) |
| `-quality-warn-batch-geohash` (`partition-048dcd40881b`) | geohash L1 | 6.56 | 2.893 | 0.446s (n=2) | 0.028s (n=1) | 0.018s (n=1) | **0.227s** | 2.004s (n=4) |
| `-quality-fail-batch-geohash` (`partition-d4afe809d1a9`) | geohash L1 | 5.76 | 2.188 | 0.188s (n=2) | 0.052s (n=1) | 0.021s (n=1) | **0.128s** | 1.431s (n=3) |

每轮产出（tiles / indexes / grid_cells，按 optical, radar, product, carbon 顺序）：

- geohash：6/6/1、6/6/1、2/2/1、100/100/1
- mgrs：2,568/2,568/428、19,536/19,536/3,256、3,920/3,920/1,960、100/100/22
- isea4h：60/60/10、222/222/37、48/48/24、100/100/3

要点：

1. **`driver.bootstrap` 0.122–0.358s**，相对历史 `partition-bd81e80f4b60` 的 **102.8s** 已消除 —— 直接证明 `PostgresPartitionJobStore.ensure_schema` 的列/索引/schema 版本预检生效。
2. 逻辑 mgrs 的写入瓶颈已从 staging 转移到 **`promote_logical_staging`：26,024 行 / 106.93s ≈ 243 rows/s**。分数据集：optical 2,568 行 18.908s、radar 19,536 行 66.262s、product 3,920 行 21.761s（三个数据集顺序执行，合计 106.9s；carbon 走碳专用路径，不经过 promote）。
3. 该吞吐明显低于微基准 88.8k rows/s，差在**目标表规模与索引维护**，不是写入模式（2026-09-12 实测）：
   - `partition_indexes`：表 489 MB / **索引 678 MB**（8 列复合唯一索引 493 MB + 主键 182 MB + `tile_output_id` 局部索引 3 MB），291,920 行，`n_dead_tup=1`；
   - `partition_tiles`：表 340 MB / **索引 804 MB**（复合唯一索引 529 MB + 主键 157 MB + searchable 局部索引 118 MB），293,338 行，`n_tup_upd=74,776`；
   - 即每一行输出都要维护两个大宽 btree（索引体量已超过表体量），这才是 243 rows/s 的真正来源；
   - 死元组几乎为 0（1 / 505），所以**不是 bloat**：`VACUUM`/`REINDEX` 收益有限，杠杆在索引列宽/数量与按 `dataset_id`/`output_version` 分区；
   - 微基准用的是全新临时表（同构索引但零体量），因此 88.8k rows/s 不代表生产大表上也能到这个量级。
   - 附：`partition_logical_staging_rows` 跑完后 `n_live_tup=0` 但仍有 46 MB 索引（TRUNCATE 不回收），也是后续可关注的写入开销。
4. isea4h L6 每数据集只有 3–37 个 cell，实体瓦片由 worker 写回对象存储；这四个数据集在实体路径下的 DB 阶段只有 `start_output`（0.02–0.83s）与 `complete_output`（0.562–3.518s/数据集，合计 6.227s），没有 `promote_logical_staging`。逻辑路径的 `complete_output` 只有 1.6–2.0s。
5. Ray Job Server 时长远大于 attempt（例如 mgrs 137.13s vs 133.26s、isea4h 174.74s vs 171.03s），差额是提交/排队/收尾开销，未计入 DB 写入统计。
6. **本轮真实运行实际走到的优化路径**：
   - `partition_domain_store._copy_insert_many`（CTAS 临时表 + COPY + 单条 `INSERT ... SELECT ... WHERE NOT EXISTS`）：实体路径 `complete_output` 写 tiles/indexes，4 个数据集各 1 次（60/222/48/100 行，均低于 5000 行阈值，因此单次调用）；
   - `promote_logical_staging` 集合式 `INSERT ... SELECT DISTINCT ON(...) ... WHERE NOT EXISTS(...)`：逻辑 geohash/mgrs/探针共 9 次；
   - `opengauss.logical_stage`（worker 侧 COPY 写入 staging）：逻辑 run 共 122 次调用；
   - 入库侧 `_copy_rows_to_temp` + 单条 `MERGE FROM (SELECT DISTINCT ON ...)`：§9.4 的 4 个数据集共 16 个 ingest_run。
   即 `69bfb52` 的 COPY / 集合式写入三处改动均已在生产链路（Ray Job + 真实 MinIO 源 + OpenGauss 真实表）上执行并产生正确行数。

### 9.4 真实入库

4 个数据集在 14:59:56.34 同时开始数据集级 ingest，全部 `completed`：

| 数据集 | output_version | ingest run 时长 | RS 表行数（`rs_ingest_job.stats_json`） | 作业级时长 |
|---|---|---|---|---|
| optical | `884d1cde004b73b209dbb844d6dd9c97` | 4.84s | `rs_cube_cell_fact` 10 / `rs_raw_scene_asset` 1 / `rs_entity_tile_asset` 10 | 0.127–1.384s |
| radar | `aa6729b40862c811f160a53cb83507d2` | 5.07s | `rs_cube_cell_fact` 37 / `rs_raw_scene_asset` 1 / `rs_entity_tile_asset` 37 | 0.134–1.033s |
| product | `b57a15c5d7b900655124a331c1e17515` | 4.00s | `rs_cube_cell_fact` 24 / `rs_raw_scene_asset` 1 / `rs_entity_tile_asset` 24 | 1.440–1.946s |
| carbon | `d96e36279119718a6970e11ff5fa6da2` | 6.34s | `rs_carbon_observation_fact` 50 | 2.745s |

- 直接按 `run_id` 聚合 RS 明细表，行数与 `stats_json` **逐项一致**；同一个 output 被重复 ingest（质量事件 + 手动请求共 16 个 ingest_run）时计数不变，说明「COPY 到临时表 + 单条 `MERGE ... DISTINCT ON`」的幂等语义在真实大表上成立。
- 场景级完成度：`ingest_run_scenes` 共 16 行全部 `completed`；`partition_run_scenes` 共 24 行全部 `completed`；`completed_scene_without_output=0`。
- 整段 `quality_ingest_seconds=15.559`（含 4 个数据集的 quality 运行 + 手动入库请求 + 等待全部完成）；KubeRay 排队/预热不在该统计内。

### 9.5 控制探针、质量与发布

- cancel 探针：`partition-6df2d86dfa0b` 取消成功（Ray Job `STOPPED`），`retry` 后 `partition-3db390ac2d73` succeeded。
- quality warn 探针（`real-accept-...-quality-warn-product`）：`warn`，`warning_count=1`、`error_count=0`，导出 `json=1`/`csv=1`，未产生 completed ingest。
- quality fail 探针（`real-accept-...-quality-fail-product`）：`fail`，`error_count=1`、`warning_count=1`，导出 `json=2`/`csv=2`，未产生 completed ingest。
- 4 个主数据集 quality：optical `pass`、product `pass`、carbon `pass`、radar `warn`（原因见 §9.6）。
- 发布生命周期：`publication_id=1c8cad53-10ea-40d5-b2f2-208623f20219`，active → withdrawn。
- Ray 证据：`backend=ray`、`execution_engine=ray_job`、`ray_job_address=http://10.3.100.183:30826`、`live_nodes=37`、`cpu=36`。

### 9.6 已知非阻塞偏差

- radar 数据集 quality 为 `warn` 而非 `pass`：源对象是 Sentinel-1 海面风场产品，6 个波段是 WindSpeed/WindDirection/IncidenceAngle/Sigma0/Mask/qc_flag，manifest 按真实语义声明 `band_type=variable`；非强制的 `radar_band_contract` 规则期望 `polarization`，因此产生 warning。验收判定接受 `pass`/`warn`，不影响通过。
- 本次 DSN 实际指向 `cube_v3` 库（`/health` 的脱敏输出可见），与 AGENTS.md 中 `postgres` 的表述不同；验收脚本两侧都走 `runtime_config.postgres_dsn()`，因此结果自洽，但文档口径需要核对。

### 9.7 关于 `product_ingest_job`（原待办 4）的决策

**本轮不改**，理由与后续建议：

1. 本轮验收里 product 数据集入库走的是 isea4h 实体输出 → `managed_output_ingest` 的 COPY + 单条 MERGE 路径（24 行 / 1.44–1.95s）。逐批 VALUES MERGE 只在 **product + logical（geohash/mgrs）** 组合生效，本轮没有真实场景量到它。
2. 按 §五 微基准，VALUES MERGE ≈ 14.9k rows/s。本轮最大的逻辑产物是 mgrs 的 26k 行/run，即使全部走该路径，与 COPY 的差距也只有秒级；当前真正的主导成本是 `promote_logical_staging` 的 ~243 rows/s（§9.3）。
3. 后续优化按收益排序（依据 §9.3 的表/索引实测）：
   - 先降 `partition_indexes` / `partition_tiles` 的索引成本（收窄 8 列复合唯一索引、评估主键与 searchable 局部索引的必要性、按 `dataset_id`/`output_version` 分区）——实测死元组极少，`VACUUM`/`REINDEX` 不是杠杆；
   - 把 `product_ingest_job.upsert_product_assets_postgres` / `upsert_product_facts_postgres` 迁移到 `ray_ingest_job` 已有的 `_copy_rows_to_temp` + 单条 MERGE 模式（纯机械改动）；
   - 给验收脚本补一个 product + logical 的入库场景，用来量测上面的改动。

### 9.8 Linear 与遗留数据

- 本次会话**没有 Linear MCP 工具**，无法写 ZAR-95 comment。待补内容：验收前缀、6 个 run 的 attempt / Ray Job ID、§9.3–§9.5 的 timings 与行数、验证命令、`status=passed` 结论。
- 验收在数据库中留下 `real-accept-0d411c506d50%` 前缀记录（datasets 7 / scenes 10 / partition_runs 6 / ingest_runs 16 / tiles 与 indexes 各 26,671 / grid_cells 5,747 / quality_runs 21）；如需清理可用脚本内的 `cleanup_sql(prefix)`，本轮**未执行**以保留证据。
- 另有一个更早的 smoke 前缀 `smoke-b2f5e0588245%`（geohash L1、仅 optical），同为可清理的测试数据。

### 9.9 复现命令（脱敏）

```bash
# 1) 受控加载运行时配置（不要打印文件内容）
set -a
. "${CUBE_WEB_ENV_FILE:-$PWD/.cube_web.env}"
set +a

# 2) 启动本地 Web API
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 127.0.0.1 --port 50039

# 3) 只读校验 manifest（逐个 stat MinIO 对象 + 命名空间化）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 \
  cube_web/scripts/run_real_partition_acceptance.py \
  --manifest /tmp/cube_accept/real-acceptance-manifest.json --prepare-only

# 4) 完整门禁（admin token 在进程内签发，不落盘、不回显）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/cube_accept/run_gate.py
```

注：`run_gate.py` 用 `CUBE_WEB_AUTH_JWT_SECRET_KEY` 在进程内签发 admin token（`role=管理员`），**没有**使用 `CUBE_WEB_AUTH_REQUIRED=0`；token 只存在于该进程环境，未写入任何文件或输出。
