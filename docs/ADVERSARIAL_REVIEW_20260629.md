# Cube Project 对抗性审查报告 v3

**审查日期**: 2026-06-29  
**最后更新**: 2026-06-29 23:53  
**审查方法**: 代码静态分析 + 运行时验证  
**审查范围**: cube_encoder / cube_split / cube_web 全仓库  
**全量测试**: 402 passed, 1 skipped, 0 failed

**文档状态**: 历史审查快照。报告中的计数、风险和路径对应 2026-06-29 代码，不应覆盖当前测试结果或运行契约。

---

## 状态图例

| 标记 | 含义 |
|------|------|
| ✅ **已修复** | 代码已修改，问题已消除 |
| ❌ **未修复** | 问题仍存在于当前代码中 |

---

## P0 — 严重

### P0-1: ray.shutdown() 无条件杀死共享 Ray 连接

| 字段 | 值 |
|------|-----|
| **状态** | ✅ **已修复** |
| **位置** | `cube_split/jobs/cancellation.py:26` → `shutdown_ray_if_needed()` |
| **修复方式** | 新增 `shutdown_ray_if_needed(ray, already_initialized)`，只在本次初始化后才 shutdown。3 个 job 文件均已改用。 |

**原问题**: 所有 Ray job 在 `finally` 块调用 `ray.shutdown()`，`ray.init()` 使用 `ignore_reinit_error=True` 导致即使 Ray 是别人初始化的，shutdown 也会杀死共享连接。

---

### P0-2: Postgres fail_attempt 丢失 manual_required 语义

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_job_store.py:1721` |

**问题**: Line 1721 SQL 硬编码 `UPDATE partition_job_attempts SET status = 'failed'`，未使用 Line 1716 计算的 `status` 变量（`"manual_required"` / `"failed"`）。

**验证结果**:
```
manual_required=True:  attempt.status='failed'   batch.status='manual_required'
manual_required=False: attempt.status='failed'   batch.status='failed'
```

真实 OpenGauss 验证: 2 个 batch 为 `manual_required`，但 attempt 表 **0 条**为 `manual_required`。

---

### P0-3: 质检 WARN 被等同 FAIL，批次被锁定

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_job_store.py:2066-2088` |

**问题**: `_apply_quality_result` 将 WARN 和 FAIL 同等对待（`if quality["quality_status"] in {"FAIL", "WARN"}`），都设置 batch status = `manual_required`。即使所有 asset 成功，仅因覆盖率 95%（WARN），batch 被锁定需要人工介入。

---

## P1 — 高

### P1-1: 自动重试覆写质检 manual_required 状态

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_workflow.py:384-409` |

**问题**: `on_task_succeeded` 中，`should_auto_retry` 在 `succeed_attempt` 之前计算。`succeed_attempt` 内部的 `_apply_quality_result` 可能将 batch 设为 `manual_required`（WARN/FAIL），但后续 `run_batch` → `create_attempt` 的 WHERE 条件不排除 `manual_required`，将其改为 `retrying`。

---

### P1-2: Slot 状态排序使用字符串而非 datetime

| 字段 | 值 |
|------|-----|
| **状态** | ✅ **已修复** |
| **修复方式** | 改用 `_parse_slot_datetime()` 函数解析 datetime |

---

### P1-3: Postgres 连接池

| 字段 | 值 |
|------|-----|
| **状态** | ✅ **已修复** |
| **修复内容** | 创建 `cube_web/services/db_pool.py`，3 个 store 均已改用连接池 |
| **配置参数** | `min_size=1, max_size=4` |

**压测结果**（真实 OpenGauss 4 节点集群）:

| 并发 | 旧 QPS (新建连接) | 新 QPS (连接池) | 加速比 |
|------|------------------|----------------|--------|
| 1 | 52 | 3820 | **73x** |
| 16 | 297 | 3968 | **13x** |
| 64 | 52 | 4178 | **80x** |

---

## P2 — 中

### P2-1: _result_implies_ingested 默认返回 True

| 字段 | 值 |
|------|-----|
| **状态** | ✅ **已修复** |
| **修复方式** | 末尾 `return True` → `return False` |

---

### P2-2: ensure_runtime_batch 无条件覆写 normalized_payload

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_job_store.py:1329` |

SQL 中 `source_schema` 有条件更新（仅对 runtime batch），但 `normalized_payload` 无条件覆写。

---

### P2-3: ISEA4H + logical 剖分时分辨率自适应被绕过

| 字段 | 值 |
|------|-----|
| **状态** | ✅ **已修复** |
| **修复方式** | 条件增加 `and method == "logical"`，ISEA4H+logical 现在有自己的分辨率自适应逻辑（<10m→8, <=30m→7, 其他→6） |

---

### P2-4: Entity actor 每次调用都 os.walk 扫描文件系统

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_split/jobs/entity_partition_job.py:529` |

`os.walk` 在 `process_groups` 方法中（line 529），而不是在 `__init__` 中。每个 actor 处理每个 task group 时都执行文件系统扫描。

---

## P3 — 低

### P3-1: asset_id 去重忽略 band 字段

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_job_store.py` |

`_stable_asset_key(source_uri, idx)` 只对 `source_uri` 做 hash，不包含 `band` 信息。同一 source_uri 不同 band 生成相同 asset_id。

---

### P3-2: extra="allow" 全局生效

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/schemas.py:9` |

所有模型继承 `extra="allow"`，拼写错误字段被静默忽略。

---

### P3-3: InMemory 与 Postgres store 行为不一致

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/partition_job_store.py:817-847` vs `2036-2060` |

两种 store 的 `_refresh_batch_from_assets` 实现路径不同，使用 InMemory 的测试覆盖不到 Postgres 特定逻辑。

---

## 第二轮扫描发现（2026-06-29 晚）

### N1: 前端 API 请求无超时保护

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/frontend/src/api/client.js` |
| **严重度** | **P2** |

`fetch()` 调用未使用 `AbortController` 或 `signal`。网络故障时前端可无限挂起。

### N2: ingest_service.py 和 health_service.py 未使用连接池

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/ingest_service.py:220`, `cube_web/services/health_service.py:115` |
| **严重度** | **P2** |

这两个模块仍使用 `psycopg.connect()` 新建连接。健康检查每次 `/health` 都新建连接。

### N3: Ingest 管道 asset_uri_map KeyError

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_split/ingest/ray_ingest_job.py:407,440` |
| **严重度** | **P2** |

`build_raw_asset_records` / `build_cube_fact_records` 在 `asset_path` 不在映射中时抛出 `KeyError`。

### N4: PDF 质检报告依赖外部命令

| 字段 | 值 |
|------|-----|
| **状态** | ❌ **未修复** |
| **位置** | `cube_web/services/quality_pdf.py` |
| **严重度** | **P3** |

PDF 生成通过 `subprocess` 调用 `libreoffice`/`wkhtmltopdf`，环境缺失时静默失败。

---

## 已验证为误报

### ~~旧版 P2-3: 自动重试链断裂后可能无限循环~~

**状态**: ❌ 误报 — 实际代码使用 dict 反向遍历 `source_task_id` 链，且有 `seen` 集合防循环，逻辑正确。

---

## 汇总

| 编号 | 问题 | 等级 | 状态 |
|------|------|------|------|
| P0-1 | ray.shutdown() 杀死共享连接 | P0 | ✅ 已修复 |
| P0-2 | fail_attempt 语义丢失 | P0 | ❌ 未修复 |
| P0-3 | 质检 WARN 锁定批次 | P0 | ❌ 未修复 |
| P1-1 | 自动重试覆写质检 | P1 | ❌ 未修复 |
| P1-2 | Slot 字符串排序 | P1 | ✅ 已修复 |
| P1-3 | 无连接池 | P1 | ✅ 已修复 |
| P2-1 | ingest 默认 True | P2 | ✅ 已修复 |
| P2-2 | payload 覆写 | P2 | ❌ 未修复 |
| P2-3 | ISEA4H 分辨率绕过 | P2 | ✅ 已修复 |
| P2-4 | os.walk 扫描 | P2 | ❌ 未修复 |
| P3-1 | asset_id 忽略 band | P3 | ❌ 未修复 |
| P3-2 | extra="allow" | P3 | ❌ 未修复 |
| P3-3 | store 不一致 | P3 | ❌ 未修复 |
| N1 | 前端无超时 | P2 | ❌ 未修复 |
| N2 | ingest/health 无池 | P2 | ❌ 未修复 |
| N3 | asset_uri_map KeyError | P2 | ❌ 未修复 |
| N4 | PDF 外部依赖 | P3 | ❌ 未修复 |

**总计**: 17 个发现，5 个已修复，12 个未修复，1 个误报已排除。

---

## 测试结果

| 包 | 测试数 | 通过 | 失败 | 跳过 |
|----|--------|------|------|------|
| cube_encoder | 92 | 92 | 0 | 0 |
| cube_split | 121 | 120 | 0 | 1 (e2e 标记) |
| cube_web | 190 | 190 | 0 | 0 |
| **总计** | **403** | **402** | **0** | **1** |

前端构建: ✅ 成功 (21.60s)

---
