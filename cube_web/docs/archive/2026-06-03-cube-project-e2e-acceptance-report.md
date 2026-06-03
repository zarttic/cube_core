# cube_project 端到端验收报告

测试日期: 2026-06-03  
工作目录: `/home/lyjdev/projects/cube_project`  
执行分支: `master`  
执行前工作区: `clean`  
最终结论: `PASS`

## 本次验收做了什么

按要求完成了以下检查、修复和复验:

1. 检查分支、工作区状态、Python 3.11 与运行时配置来源。
2. 通过健康检查等价代码路径确认 PostgreSQL、Ray、MinIO、bucket、配置解析全部可用。
3. 使用真实 Ray/PostgreSQL/MinIO 跑全链路 smoke，并在代码修复后再次复跑全量 smoke。
4. 运行完整自动化测试:
   ```bash
   PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
   cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
   ```
5. 追加回归测试，覆盖:
   - zero-valued masked sample 的质检分类
   - Ray source cache 不可写时的 fallback
   - hashed COG warning path 的 retry 匹配
   - 同一 `asset_id` 导入新 batch 时的资产重挂与状态重置
6. 使用真实持久化工作流复跑 `WARN -> manual_retry -> PASS`，并故意复用历史出过问题的 `asset-good` / `asset-warn`，验证:
   - `partition_assets` 能迁移到新 batch
   - `manual_retry` 只重试告警资产
   - retry 期间 stale quality 字段被清空
   - 最终 batch 回到 `PASS`

## 运行时配置来源与健康检查

健康检查结果: `PASS`

- PostgreSQL: `ok`
- Ray: `ok`，4 个节点可用
- MinIO: `ok`
- Bucket: `ok`
- 配置来源:
  - `CUBE_WEB_POSTGRES_DSN`: `.cube_web.env`
  - `CUBE_WEB_RAY_ADDRESS`: `.cube_web.env`
  - `CUBE_WEB_MINIO_ENDPOINT`: `.cube_web.env`
  - `CUBE_WEB_MINIO_BUCKET`: `.cube_web.env`
  - `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`: MinIO service env

证据:

- `/tmp/cube_acceptance_20260603/health_report.json`

## 验收结果

### 1. 修复后的真实 smoke

修复后重新执行:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_split/scripts/run_all_partition_flows_smoke.py \
  --mode demo \
  --ray-parallelism 2 \
  --chunk-size 1 \
  --max-cells-per-asset 50 \
  --keep-quality \
  --work-dir /tmp/cube_acceptance_20260603/work_after_fixes \
  --summary-path /tmp/cube_acceptance_20260603/smoke_summary_after_fixes.json \
  --run-id acceptance_20260603_fix4
```

结果: `PASS`

| 项目 | 结果 | batch_id | report_id | 说明 |
| --- | --- | --- | --- | --- |
| optical geohash | PASS | `smoke-acceptance_20260603_fix4-optical_geohash` | `e64fa8e4-9bb7-4917-b6c0-ecfe10ce7fe0` | 真实 Ray 执行，`rows=1`，`quality_status=PASS` |
| optical MGRS | PASS | `smoke-acceptance_20260603_fix4-optical_mgrs` | `3b4da68a-fdcc-4b01-897c-7ab4a04381c7` | 真实 Ray 执行，`rows=6` |
| optical ISEA4H level 1 | PASS | `smoke-acceptance_20260603_fix4-optical_isea4h_level1` | `007cb242-50a2-46d4-b716-9fcf9e488dee` | 真实 Ray 实体剖分，`uploaded_tile_count=1`，`metadata_rows=1` |
| product geohash | PASS | `smoke-acceptance_20260603_fix4-product_geohash` | `7093a9fc-b33f-4529-9b60-050c01c7ad0b` | 真实 Ray 执行，`rows=2`，`quality_status=PASS` |
| carbon satellite | PASS | `smoke-acceptance_20260603_fix4-carbon_satellite` | `74a1aaa1-9f27-4a52-af69-57d2bcec3f1b` | 真实 Ray 执行，`rows=1`，`quality_status=PASS` |
| AOI readback | PASS | 复用 `optical_geohash` | N/A | 输出 `/tmp/cube_acceptance_20260603/work_after_fixes/acceptance_20260603_fix4/aoi_readback/optical_geohash_aoi.tif`，`97x96x1` |

说明:

- smoke 脚本直接调用 runner，不经过 Web 异步任务编排，因此无 `task_id`。
- 修复前的初次 smoke 基线仍保留在 `/tmp/cube_acceptance_20260603/smoke_summary.json`。
- 修复后的最终 smoke 汇总以 `/tmp/cube_acceptance_20260603/smoke_summary_after_fixes.json` 为准。

### 2. 真实持久化 WARN -> manual_retry -> PASS

结果: `PASS`

真实工作流批次:

- `batch_id`: `ACCEPTANCE_REAL_WARN_RETRY_realwarnretry_fix4_20260603015206`
- 初次任务: `partition-2e0f3fc2ae5f`
- 手动重试任务: `partition-ca6531c00687`

关键状态流转:

| 阶段 | 结果 | 关键字段 |
| --- | --- | --- |
| 初次运行 | WARN | `quality_report_id=05bc51ab-d2b7-4f74-aa4b-d0ae6d4bd0d5`，`quality_failure_reason=pixel_sample: Some assets have zero-valued sample windows.` |
| 重试提交后 | retrying | `quality_status=null`，`quality_report_id=null`，`quality_failure_reason=null` |
| 最终完成 | PASS | `quality_report_id=8a15d539-cd34-4a1c-8b50-94131c24cd89` |

关键验收点:

- `partition_assets` 在导入阶段已经正确挂到当前 batch:
  - `asset-good.batch_id = ACCEPTANCE_REAL_WARN_RETRY_realwarnretry_fix4_20260603015206`
  - `asset-warn.batch_id = ACCEPTANCE_REAL_WARN_RETRY_realwarnretry_fix4_20260603015206`
- `manual_retry` 的 attempt 记录为:
  - `operation=manual_retry`
  - `retry_strategy=quality_warning_assets`
  - `asset_ids=["asset-warn"]`
- 资产 attempt 次数符合预期:
  - `asset-good.attempt_count = 1`
  - `asset-warn.attempt_count = 2`

证据:

- 最终通过证据: `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed4.json`
- 修复前中间态证据:
  - `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance.json`
  - `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed.json`
  - `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed2.json`
  - `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed3.json`

### 3. 质量报告与状态持久化

结果: `PASS`

已有真实与测试双重证据:

- 真实 runner 结果包含 `quality_status` / `quality_report_id` / `quality_report`:
  - `/tmp/cube_acceptance_20260603/runner_quality_field_check.json`
- 真实 `quality_reports` 落库回查:
  - `/tmp/cube_acceptance_20260603/quality_report_store_lookup.json`
- API 风格状态流转证据:
  - `/tmp/cube_acceptance_20260603/manual_retry_acceptance.json`
  - `/tmp/cube_acceptance_20260603/asset_retry_acceptance.json`

## 自动化测试结果

### 跨包测试

命令:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```

结果: `177 passed, 1 skipped in 48.51s`

### Web 测试

命令:

```bash
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
```

结果: `118 passed, 1 warning in 8.21s`

### 本轮新增/聚焦回归

命令:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_split/tests/test_quality_check.py \
  cube_split/tests/test_ray_partition_core.py \
  cube_web/tests/test_app.py \
  -k 'test_quality_check_marks_zero_masked_samples_as_warn_not_fail or test_download_s3_object_falls_back_to_user_writable_cache or test_optical_partition_retry_endpoint_matches_hashed_cog_warning_assets or test_partition_job_store_reassigns_duplicate_asset_id_to_new_batch or test_partition_batch_quality_warn_enters_manual_queue_and_retries_warning_asset or test_partition_batch_quality_warn_retry_matches_hashed_cog_warning_assets' -q
```

结果: `6 passed`

## 发生了什么问题

本轮验收过程中实际发现并修复了 4 个代码问题:

1. 光学质检对 zero-valued masked sample 的分类错误。  
   真实 `WARN` 场景里，全 0 且 `nodata=0` 的样本窗口被当成 `asset_readability FAIL`，而不是 `pixel_sample WARN`。这会让批次表现成错误的失败类型，干扰手动重试链路。

2. Ray worker 的 source cache 可能因为历史 root-owned 残留目录而不可写。  
   真实重试时曾出现:
   ```text
   Permission denied: /tmp/cube_split_source_cache/...part.minio
   ```
   这会导致本应可重试的任务直接卡死在本地缓存下载阶段。

3. PostgreSQL `partition_assets` 的 `asset_id` 冲突更新没有迁移 `batch_id`。  
   当同一个 `asset_id` 被再次导入到新 batch 时，资产记录仍挂在旧 batch，导致当前 batch 的 `list_assets(batch_id)` 为空，真实 `manual_retry` 无法定位告警资产，退化成 `full_batch`。

4. hashed COG warning path 的匹配逻辑只修到了 `partition_runners.py`，没有同步修到 `partition_workflow.py`。  
   因此即使第 3 个问题修掉，真实工作流还是会把:
   - `warn_zero_optical_<digest>_cog.tif`
   看成匹配失败，`manual_retry` 仍然选择整批重跑。`/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed3.json` 记录了这个中间失败态。

## 修改了什么

### 业务代码

- `cube_split/cube_split/quality/optical_quality.py`
  - `_validate_assets(...)` 新增 `source_cache_dir` 参数透传。
  - 对 masked sample 改为按 `valid_mask` 统计 `valid_count` 与 `nonzero_count`，避免把全 0 masked 数据误判为可读性失败。

- `cube_split/cube_split/jobs/ray_partition_core.py`
  - 新增 `_fallback_cache_root(...)` 与 `_cache_target_for_uri(...)`。
  - 当默认 cache 根目录不可写时，自动回退到用户可写的 `/tmp/<cache_root>_u<uid>`。

- `cube_web/cube_web/services/partition_job_store.py`
  - `InMemoryPartitionJobStore.upsert_schema()` 在同一 `asset_id` 导入到新 batch 时，重置 `status`、`attempt_count`、`last_error`、`last_run_dir`、`partitioned_at`。
  - `PostgresPartitionJobStore.upsert_schema()` 的 `ON CONFLICT (asset_id)` 补充更新:
    - `batch_id`
    - `data_type`
    - 同 batch 保留状态，新 batch 重置状态与计数

- `cube_web/cube_web/services/partition_runners.py`
  - `_asset_matches_warning_path(...)` 支持匹配 `asset_b_<digest>_cog.tif` 这类 hashed COG 文件名。

- `cube_web/cube_web/services/partition_workflow.py`
  - `_asset_matches_warning_path(...)` 同步补齐 hashed COG 匹配逻辑，保证真实 `manual_retry` 也能选中 warning 资产，而不是只修 endpoint 级 retry。

### 测试

- `cube_split/tests/test_quality_check.py`
  - 新增 `test_quality_check_marks_zero_masked_samples_as_warn_not_fail`

- `cube_split/tests/test_ray_partition_core.py`
  - 新增 `test_download_s3_object_falls_back_to_user_writable_cache`

- `cube_web/tests/test_app.py`
  - 新增 `test_partition_job_store_reassigns_duplicate_asset_id_to_new_batch`
  - 新增 `test_optical_partition_retry_endpoint_matches_hashed_cog_warning_assets`
  - 新增 `test_partition_batch_quality_warn_retry_matches_hashed_cog_warning_assets`

### 文档

- 本报告:
  - `cube_web/docs/archive/2026-06-03-cube-project-e2e-acceptance-report.md`
- 导出的 `.docx` 版本:
  - `/tmp/cube_acceptance_20260603/docx_export/2026-06-03-cube-project-e2e-acceptance-report.docx`

## 非阻断告警

1. `requests` 仍会输出 `RequestsDependencyWarning`，提示 `urllib3/chardet` 组合不在其声明支持范围内；健康检查、真实 smoke、真实工作流与测试均未受影响。
2. `cube_web/tests` 仍存在 `StarletteDeprecationWarning`，提示 `starlette.testclient` 与 `httpx` 的未来兼容性问题；当前 118 个 Web 测试全部通过。

## 产物与证据清单

- 健康检查:
  - `/tmp/cube_acceptance_20260603/health_report.json`
- 初次 smoke 基线:
  - `/tmp/cube_acceptance_20260603/smoke_summary.json`
- 修复后最终 smoke:
  - `/tmp/cube_acceptance_20260603/smoke_summary_after_fixes.json`
- 真实质量报告落库回查:
  - `/tmp/cube_acceptance_20260603/quality_report_store_lookup.json`
- 真实 runner 字段校验:
  - `/tmp/cube_acceptance_20260603/runner_quality_field_check.json`
- API 风格手动重试证据:
  - `/tmp/cube_acceptance_20260603/manual_retry_acceptance.json`
- API 风格资产级重试证据:
  - `/tmp/cube_acceptance_20260603/asset_retry_acceptance.json`
- 真实持久化 WARN/manual_retry 修复链路:
  - `/tmp/cube_acceptance_20260603/real_warn_retry_acceptance_fixed4.json`
- 文档导出:
  - `/tmp/cube_acceptance_20260603/docx_export/2026-06-03-cube-project-e2e-acceptance-report.docx`

以上临时产物均未加入 Git。
