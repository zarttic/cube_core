# 获取载入批次对应的剖分状态

更新时间：2026-08-25

本文说明如何根据正式载入批次 `load_batch_id` 获取其数据对应的剖分状态。这里的“剖分
状态”是数据单元在指定格网类型和格网层级下的状态，不是载入批次本身的
`status`，也不是一次剖分运行的整体状态。

## 1. 先明确三个 ID 和状态粒度

系统中这几个对象不是一一对应关系：

```text
load_batch_id
  └─ load_batch_scenes.scene_id
       └─ scene_bands.band_unit_id
            └─ partition_data_unit_grid_status
                 ├─ grid_type + grid_level
                 ├─ partition_status / quality_status / ingest_status
                 └─ partition_run_id
```

- `load_batch_id`：一次正式载入动作的批次 ID。子系统导入和数据集重新载入都使用这个
  身份。
- `partition_run_id`：一次提交的剖分运行 ID。一次运行可以合并多个载入批次；因此不能
  把它直接当成 `load_batch_id`。
- `band_unit_id`：最小数据单元 ID。剖分状态的实际粒度是
  `band_unit_id + grid_type + grid_level`。
- `partition_data_unit_grid_status` 没有直接保存 `load_batch_id`。查询载入批次时，先
  通过 `load_batch_scenes -> scene_bands` 找到批次内的数据单元，再读取该数据单元的
  `grid_statuses`。

同一个 Scene 或波段如果被多个载入批次复用，它们可能返回同一条数据单元格网状态。该
状态描述的是当前数据单元和格网配置的生产状态，不表示某个历史载入批次拥有一份独立的
剖分结果。

## 2. 推荐 API 调用流程

### 2.1 已知载入批次 ID：直接查询批次数据和状态

使用以下接口获取批次内的 Dataset、Scene、波段以及每个波段的格网状态：

```http
GET /v1/partition/load-batches/{load_batch_id}/scenes
```

示例：

```bash
BASE_URL="http://127.0.0.1:50039"
LOAD_BATCH_ID="LOAD_20260825_001"

curl -sS \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${BASE_URL}/v1/partition/load-batches/${LOAD_BATCH_ID}/scenes"
```

认证开启时，除公开的 schema 导入接口外，`/v1/*` 都需要 Bearer Token。不要把真实
Token、OpenGauss DSN 或对象存储凭据写入文档或命令历史。

接口支持以下筛选参数：

| 参数 | 作用 |
| --- | --- |
| `status` | 过滤 `load_batch_scenes.load_status`，例如 `succeeded`；不是剖分状态筛选。 |
| `data_type` | 过滤 Dataset 类型，例如 `optical`、`radar`、`product`、`carbon`。 |
| `dataset_id` | 只返回指定 Dataset 的 Scene。 |

例如，只查看某批次中已成功载入的光学数据：

```bash
curl -sS \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${BASE_URL}/v1/partition/load-batches/${LOAD_BATCH_ID}/scenes?status=succeeded&data_type=optical"
```

`load_batch.scene_count` 是整个批次的 Scene 总数；响应顶层的 `scene_count` 是本次接口
调用经过 `status`、`data_type`、`dataset_id` 筛选后实际返回的 Scene 数量。未传筛选参数时，
两者通常相同；带筛选参数时不要用批次总数代替当前响应数量。

### 2.2 响应中剖分状态的位置

响应的主要结构如下，实际返回还可能包含时间、空间范围、资产和分辨率等元数据：

```json
{
  "load_batch": {
    "load_batch_id": "LOAD_20260825_001",
    "batch_name": "山东光学数据",
    "status": "succeeded",
    "dataset_count": 1,
    "scene_count": 1
  },
  "datasets": [
    {
      "dataset_id": "dataset-optical-001",
      "dataset_code": "OPTICAL-001",
      "data_type": "optical",
      "scenes": [
        {
          "scene_id": "scene-001",
          "load_batch_id": "LOAD_20260825_001",
          "load_status": "succeeded",
          "bands": [
            {
              "band_unit_id": "band-001-b04",
              "band_code": "B04",
              "grid_statuses": [
                {
                  "grid_type": "geohash",
                  "grid_level": 6,
                  "partition_run_id": "partition-run-001",
                  "partition_status": "completed",
                  "quality_status": "pending",
                  "ingest_status": "pending",
                  "output_version": "output-001",
                  "attempt_no": 1,
                  "error_message": null
                },
                {
                  "grid_type": "mgrs",
                  "grid_level": 1,
                  "partition_run_id": null,
                  "partition_status": "pending",
                  "quality_status": "pending",
                  "ingest_status": "pending",
                  "output_version": null,
                  "attempt_no": 0,
                  "error_message": null
                }
              ]
            }
          ]
        }
      ]
    }
  ],
  "scene_count": 1
}
```

获取指定格网状态时，必须同时匹配 `grid_type` 和 `grid_level`：

```js
function partitionStatuses(response, { gridType, gridLevel } = {}) {
  return (response.datasets || []).flatMap((dataset) => (
    (dataset.scenes || []).flatMap((scene) => (
      (scene.bands || []).flatMap((band) => (
        (band.grid_statuses || [])
          .filter((status) => !gridType || status.grid_type === gridType)
          .filter((status) => gridLevel == null || Number(status.grid_level) === Number(gridLevel))
          .map((status) => ({
            dataset_id: dataset.dataset_id,
            scene_id: scene.scene_id,
            band_unit_id: band.band_unit_id,
            ...status,
          }))
      ))
    ))
  ));
}

const rows = partitionStatuses(response, { gridType: 'geohash', gridLevel: 6 });
```

如果指定的 `band_unit_id + grid_type + grid_level` 没有出现在 `grid_statuses` 中，说明
该组合尚未建立持久化状态记录。页面可将其展示为“待剖分/未剖分”，但不能将“没有记录”
解释为“剖分失败”。如果需要统计一个批次的状态，应先确定目标格网配置，再对状态记录按
`partition_status` 聚合；不能把不同格网层级混在一起统计。

## 3. 状态字段含义

### 3.1 数据单元剖分状态

`band.grid_statuses[].partition_status` 的当前枚举为：

| 值 | 业务含义 |
| --- | --- |
| `pending` | 已登记或等待处理，尚未进入执行。 |
| `queued` | 已排队，等待执行资源。 |
| `running` | 正在剖分。 |
| `completed` | 该数据单元在该格网配置下已完成剖分并产生输出版本。 |
| `failed` | 当前剖分尝试失败；结合 `error_message` 和 `attempt_no` 定位原因。 |
| `cancelled` | 当前剖分被取消。 |

`completed` 只表示剖分完成，不表示已经通过质检或完成入库。应同时查看：

- `quality_status`：`pending`、`running`、`pass`、`warn`、`fail`、`error`、`cancelled`。
- `ingest_status`：`pending`、`queued`、`running`、`completed`、`failed`、`cancelled`。

因此，一个常用的业务判断是：

```text
partition_status = completed
  且 quality_status ∈ {pass, warn}
  且 ingest_status = completed
  => 该数据单元该格网配置已完成全流程
```

仅满足 `partition_status = completed` 时，通常仍处于“待质检”；质检通过后但尚未入库时，
通常处于“待入库”。

### 3.2 载入状态和剖分状态不要混用

`load_batch.status` 表示批次层面的载入生命周期，`scene.load_status` 表示批次与具体
Scene 关联的载入结果。两者均不是 `partition_status`：

```text
load_batch.status = succeeded
  => 批次载入登记成功

band.grid_statuses[].partition_status = completed
  => 指定数据单元、格网类型、格网层级的剖分成功
```

一个批次可以已经成功载入，但其数据仍然没有剖分；反过来，剖分状态也不能替代载入状态。

## 4. 需要剖分运行整体状态时

如果已经从 `grid_statuses` 取得 `partition_run_id`，可以查询剖分运行列表：

```http
GET /v1/partition/runs?keyword={load_batch_id}&page=1&page_size=500
```

`keyword` 会匹配剖分运行 ID、来源载入批次 ID/名称以及 Dataset 标识，因此客户端仍应
使用精确匹配确认：

```js
const candidates = response.items || [];
const runsForLoadBatch = candidates.filter((run) => (
  (run.source_load_batch_ids || []).includes(loadBatchId)
));
```

运行列表中的常用字段包括：

| 字段 | 含义 |
| --- | --- |
| `partition_run_id` | 剖分运行 ID。 |
| `status` | 运行整体状态：`pending`、`queued`、`running`、`completed`、`partial_failure`、`failed`、`cancelled`。 |
| `source_load_batch_ids` | 本次运行合并的来源载入批次 ID 列表。 |
| `band_count` | 本次运行关联的状态数据单元数量。 |
| `partitioned_count` | 本次运行中 `partition_status=completed` 的数据单元数量。 |
| `partition_failed_count` | 本次运行中剖分失败的数据单元数量。 |
| `quality_pass_count` / `quality_failed_count` | 本次运行中质检通过/失败的数据单元数量。 |
| `ingested_count` / `ingest_failed_count` | 本次运行中已入库/入库失败的数据单元数量。 |

例如：

```json
{
  "items": [
    {
      "partition_run_id": "partition-run-001",
      "status": "completed",
      "source_load_batch_ids": ["LOAD_20260825_001"],
      "source_load_batch_names": ["山东光学数据"],
      "band_count": 2,
      "partitioned_count": 2,
      "partition_failed_count": 0,
      "quality_pass_count": 0,
      "quality_failed_count": 0,
      "ingested_count": 0,
      "ingest_failed_count": 0
    }
  ],
  "total": 1,
  "page": 1,
  "page_size": 500
}
```

注意：运行列表的计数是整个 `partition_run_id` 的计数。如果一次运行合并了多个来源
载入批次，这些计数不能当作某一个 `load_batch_id` 的独立计数；单个载入批次仍应以
`GET /load-batches/{load_batch_id}/scenes` 返回的 `grid_statuses` 为准。

需要查看某个运行的详细数据单元和质检信息时，再调用：

```http
GET /v1/partition/runs/{partition_run_id}/quality
```

## 5. 载入批次列表接口的使用边界

### 5.1 查询列表

```http
GET /v1/partition/load-batches
```

常用参数为 `status`、`data_type`、`keyword`、`dataset_id`、`page` 和 `page_size`。响应
同时提供 `items` 和兼容字段 `load_batches`，二者表示同一批次列表。

没有传 `status` 时，当前接口按“待剖分批次列表”语义查询，排除已经成功或归档的批次，
并要求批次中仍有未完成入库的数据单元。传 `status=succeeded` 也仍会应用这个“有待处理
数据”的条件，因此已经全部完成入库的历史批次可能不会出现在列表中。

如果已经知道批次 ID，应直接调用单批次接口，而不要依赖列表接口找回已完成历史批次。

### 5.2 单批次元数据接口不返回剖分明细

```http
GET /v1/partition/load-batches/{load_batch_id}
```

该接口用于确认批次存在并读取批次元数据、Dataset 数量和 Scene 数量。它不返回波段的
`grid_statuses`，因此不能用它判断剖分是否完成。要获取剖分状态，必须继续调用
`/{load_batch_id}/scenes`。

## 6. 运维核对 SQL（只读）

API 不可用或需要在 OpenGauss 中核对链路时，可执行下面的只读核对查询。查询已包含
`dataset_reload` 批次的 `reload_selection.band_unit_ids` 过滤，因此只返回该重新载入批次
实际选择的波段。实际应用中请使用 psycopg 的参数绑定，不要拼接用户输入；`%s` 是 psycopg
参数占位符：

```sql
SELECT
    lbs.load_batch_id,
    d.dataset_id,
    s.scene_id,
    sb.band_unit_id,
    sb.band_code,
    g.grid_type,
    g.grid_level,
    g.partition_run_id,
    COALESCE(g.partition_status, 'pending') AS partition_status,
    COALESCE(g.quality_status, 'pending') AS quality_status,
    COALESCE(g.ingest_status, 'pending') AS ingest_status,
    g.output_version,
    g.attempt_no,
    g.error_message
FROM load_batch_scenes lbs
JOIN load_batches lb ON lb.load_batch_id = lbs.load_batch_id
JOIN scenes s ON s.scene_id = lbs.scene_id
JOIN datasets d ON d.dataset_id = s.dataset_id
JOIN scene_bands sb ON sb.scene_id = s.scene_id
JOIN scene_assets sa
  ON sa.scene_id = sb.scene_id
 AND sa.asset_id = sb.asset_id
 AND sa.asset_role = 'data'
LEFT JOIN partition_data_unit_grid_status g
  ON g.band_unit_id = sb.band_unit_id
WHERE lbs.load_batch_id = %s
  AND lb.status <> 'archived'
  AND (
      COALESCE(lb.source_type, 'subsystem_import') <> 'dataset_reload'
      OR NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
              COALESCE(lb.attributes->'reload_selection'->'datasets', '[]'::jsonb)
          ) selected_dataset
          WHERE selected_dataset->>'dataset_id' = d.dataset_id
      )
      OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
              COALESCE(lb.attributes->'reload_selection'->'datasets', '[]'::jsonb)
          ) selected_dataset
          WHERE selected_dataset->>'dataset_id' = d.dataset_id
            AND EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(
                    COALESCE(selected_dataset->'band_unit_ids', '[]'::jsonb)
                ) selected_band(band_unit_id)
                WHERE selected_band.band_unit_id = sb.band_unit_id
            )
      )
  )
ORDER BY d.dataset_id, s.scene_id, sb.band_unit_id, g.grid_type, g.grid_level;
```

使用 psycopg 执行时，参数应作为第二个参数传入，例如
`cursor.execute(sql, (load_batch_id,))`。如果使用数据库客户端而不是 psycopg，请按该客户
端的参数语法替换 `%s`，但仍要保留重新载入批次的波段过滤条件。

这里的 `LEFT JOIN` 用于保留尚未建立任何格网状态记录的波段；这类行应显示为“待剖分”，
而不是“剖分失败”。生产应用仍应优先调用 Web API，以复用认证、归档过滤和 Dataset/Scene
聚合规则。

## 7. 常见错误

1. 把 `load_batch.status=succeeded` 当成剖分完成。它只说明载入批次成功。
2. 调用 `/load-batches/{id}` 后没有继续调用 `/scenes`，因此看不到波段格网状态。
3. 把 `/scenes?status=completed` 当成剖分状态筛选。该参数过滤的是 `load_status`，而
   `partition_status` 需要在 `bands[].grid_statuses[]` 中读取。
4. 只按 `grid_type` 查状态而忽略 `grid_level`。同一波段可以存在多个格网层级记录。
5. 看到 `partition_status=completed` 就认为已入库。还必须检查 `quality_status` 和
   `ingest_status`。
6. 直接使用运行整体的 `status` 或统计数字代表单个载入批次。一次运行允许合并多个来源
   载入批次，批次级判断应回到 `load_batch_id -> scene -> band -> grid_status` 链路。

## 8. 契约来源

- API 路由：[scene_partition.py](../cube_web/routes/scene_partition.py)
- 载入批次查询和状态关联：[scene_repository.py](../cube_web/services/scene_repository.py)
- 领域表和状态枚举：[scene_domain_schema.py](../cube_web/services/scene_domain_schema.py)
- API 回归测试：[test_scene_api.py](../tests/test_scene_api.py)
- 载入批次与剖分批次关系：[MULTI_GRID_PARTITION_DESIGN.md（已归档）](../../archive/docs/MULTI_GRID_PARTITION_DESIGN.md)
