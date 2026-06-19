# Web 剖分续跑交接（2026-06-19）

## 端口约束

- 后端固定端口：`50039`
- 前端固定端口：`50040`
- 不再使用其他临时端口，尤其不要再启 `50041`
- 当前已确认：
  - `50039` 有后端进程在监听
  - `50040` 有前端进程在监听
  - `50041` 已停止

## 当前分支

- `feat/ard-loader-schema-integration`

## 本轮已完成的代码改动

为保证“像人在 Web 上点击提交任务”这条链路稳定，我已完成两处运行态收紧：

1. [cube_web/cube_web/services/partition_job_store.py](/home/lyajun/projects/cube_project/cube_web/cube_web/services/partition_job_store.py:809)
   - `PostgresPartitionJobStore.ensure_schema()` 改为进程内只真正执行一次
   - 避免每次查队列/任务详情都重复跑 DDL

2. [cube_web/cube_web/services/partition_workflow.py](/home/lyajun/projects/cube_project/cube_web/cube_web/services/partition_workflow.py:20)
   - 任务执行中的 `cancellation_check` 改为 1 秒节流
   - 避免 worker 循环频繁访问 OpenGauss

3. 新增验证测试：
   - [cube_web/tests/test_app.py](/home/lyajun/projects/cube_project/cube_web/tests/test_app.py:1507)
   - [cube_web/tests/test_app.py](/home/lyajun/projects/cube_project/cube_web/tests/test_app.py:1985)

4. 测试结果：
   - `cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests`
   - 结果：`149 passed`

## 当前外部环境阻塞

### 1. OpenGauss 当前不可正常连接

当前 `.cube_web.env` 中 DSN 为：

- `postgresql://remote_user:Remote2026@10.3.100.180:15400/postgres?client_encoding=UTF8`

本轮实测结果：

- `10.3.100.179:15400`：TCP 通，但 PG 握手超时
- `10.3.100.180:15400`：TCP 通，但 PG 握手超时
- `10.3.100.182:15400`：TCP 通，但 PG 握手超时
- `10.3.100.181:15400`：能返回 PG 错误，但拒绝当前 `remote_user/Remote2026`

这意味着当前 Web 队列表、任务详情、运行态落库都无法继续正常使用。代码侧已经收紧，但数据库连接问题仍然存在。

### 2. Ray 分布式节点异常

当前 Ray 集群状态：

- `10.3.100.182`：`ALIVE`
- `10.3.100.179`：`ALIVE`
- `10.3.100.181`：`ALIVE`
- `10.3.100.180`：`DEAD`

Ray 状态给出的原因：

- `Unexpected termination: health check failed due to missing too many heartbeats`

这会直接影响资产并行任务的稳定性，尤其是 tile / optical asset 这类会分发到多个 actor 的任务。

## 已完成的清理

在重新走 Web 提交流程之前，已经清掉上一轮“非 Web 队列方式”的验证数据：

- DB 已删：
  - `rs_cube_cell_fact`
  - `rs_raw_scene_asset`
  - `rs_product_cell_fact`
  - `rs_product_asset`
  - `rs_entity_tile_asset`
  - `rs_carbon_observation_fact`
  - `rs_ingest_job`
- MinIO 已删：
  - `cube/prod_validate/optical_s2/raw/dataset=prodval_optical_s2_20260618/`
  - `cube/prod_validate/optical_tile/raw/dataset=prodval_optical_tile_20260618/`
  - `cube/prod_validate/product/raw/dataset=dianzhong_ecological_security/` 下 `/version=product_20260618/`
  - `cube/prod_validate/entity/dataset=prodval_optical_entity_20260618/`
  - `cube/entity_cog/dataset=prodval_optical_entity_20260618/`

## 本轮按 Web 提交流程已执行到的状态

说明：以下任务是按 Web `run` 接口提交的，不是直接跑底层 job。

### 1. Optical S2

- 批次：`WEBRUN_OPTICAL_S2_20260618`
- 任务：`partition-f52993fd7b91`
- 路由：`POST /v1/partition/optical/tasks/run`
- 结果：已确认成功
- 已知结果：
  - `rows=36`
  - `asset_count=3`
  - 之前已确认在任务队列中可见

### 2. Optical 实体剖分

- 批次：`WEBRUN_OPTICAL_ENTITY_20260618`
- 任务：`partition-cf23a93eeea3`
- 路由：`POST /v1/partition/optical/tasks/run`
- 参数特征：`grid_type=isea4h`
- 结果：已确认成功
- 已知结果：
  - `rows=6224`
  - `asset_count=1`
  - 之前已确认在任务队列中可见

### 3. Optical tile

- 批次：`WEBRUN_OPTICAL_TILE_20260618`
- 任务：`partition-24dc5ee4ebfc`
- 路由：`POST /v1/partition/optical/tasks/run`
- 当前判断：未成功收口，需重跑

补充依据：

- 在 Ray 最近任务中，对应最新 asset 并行任务出现：
  - `job_id=38000000`
  - `AssetTaskProcessor.process_groups`
  - `FAILED`
  - `ACTOR_DIED`
  - 原因：actor 落在 `10.3.100.180`，随后节点死亡

因此 tile 这一批不要视为完成。

### 4. Product

- 批次预留：`WEBRUN_PRODUCT_20260618`
- 当前状态：本轮尚未完成重新提交

### 5. Carbon

- 批次预留：`WEBRUN_CARBON_20260618`
- 当前状态：本轮尚未完成重新提交

## 本轮使用的当前系统源数据

### Optical S2（3 个）

- `s3://cube/cube/source/optocal/Shandong_mosaic_2015Q3_sr_band3_cut/Shandong_mosaic_2015Q3_sr_band3_cut.tif`
- `s3://cube/cube/source/optocal/Shandong_mosaic_2017Q2_sr_band2_cut/Shandong_mosaic_2017Q2_sr_band2_cut.tif`
- `s3://cube/cube/source/optocal/Shandong_mosaic_2017Q3_sr_band3_cut/Shandong_mosaic_2017Q3_sr_band3_cut.tif`

### Optical 实体剖分（1 个）

- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif`

### Optical tile（1 个）

- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band4_cut/Shandong_mosaic_2020Q3_sr_band4_cut.tif`

### Product（5 个）

- `s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif`
- `s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_1990年.tif`
- `s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_2000年.tif`
- `s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_2010年.tif`
- `s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_2020年.tif`

### Carbon（1 个）

- `s3://cube/cube/source/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4`

## 恢复后必须遵循的操作边界

1. 只走 Web 提交流程
   - 使用 `/v1/partition/{data_type}/tasks/run`
   - 不再直接调用底层 partition / ingest job 作为正式验证路径

2. 只使用 `50039` 和 `50040`
   - 后端只认 `50039`
   - 前端只认 `50040`
   - 不再启动 `50041` 或其他临时端口

3. 只用“当前系统”的源数据
   - 不混入其他系统数据

4. 队列检查要低频
   - 不要再高频打 `/v1/partition/tasks` 和 `/v1/partition/tasks/{task_id}`
   - 优先单次确认，必要时拉大间隔

## 用户测试完成后，恢复操作顺序

### 前提

用户测试后，需要至少满足以下一项：

- 提供新的可用 OpenGauss DSN
- 或把现有 OpenGauss 主库/账号恢复到可连接可写状态

最好同时恢复：

- Ray `10.3.100.180` 节点

### 恢复步骤

1. 先阅读本文件
   - [docs/WEB_PARTITION_RESUME_20260619.md](/home/lyajun/projects/cube_project/docs/WEB_PARTITION_RESUME_20260619.md)

2. 确认端口
   - 后端：`50039`
   - 前端：`50040`

3. 先验证基础设施
   - OpenGauss 可连接
   - Ray 节点状态恢复

4. 再验证 Web 队列接口
   - `/v1/partition/tasks`
   - `/v1/partition/tasks/{task_id}`

5. 重新走未完成批次
   - `WEBRUN_OPTICAL_TILE_20260618`
   - `WEBRUN_PRODUCT_20260618`
   - `WEBRUN_CARBON_20260618`

6. 最终验收
   - 任务队列可见
   - 运行态表可查询
   - MinIO 产物存在
   - 必要时补验 `ard_partition_*` 对账侧数据

## 本次停点结论

现在不继续往下跑，不是因为 Web 链路代码还没处理完，而是因为外部环境已经成为硬阻塞：

- OpenGauss 当前不可用
- Ray `10.3.100.180` 当前为 `DEAD`

代码侧的稳定性修复和测试已完成，恢复时直接从这里接着继续即可。
