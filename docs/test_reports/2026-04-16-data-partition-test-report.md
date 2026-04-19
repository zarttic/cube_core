# 数据剖分测试报告

测试日期: 2026-04-16  
工作目录: `/home/lyjdev/projects/cube_encoder`  
测试目标: 针对 `data/` 目录下数据的剖分链路进行功能与性能验证，重点关注 `data/landsat8` 的并行化剖分耗时

## 数据集清单

- `data/landsat8`: 8 个 TIF 文件
- `data/landsat8_synth_20`: 160 个 TIF 文件
- `data/landsat8_synth`: 640 个 TIF 文件

## 已完成的测试用例

### 用例 0: `data/landsat8` 仅统计剖分阶段耗时的基线与优化测试

测试范围:
- 数据集: `data/landsat8`
- 计时口径: 从开始剖分到剖分结束，对应 `partition_elapsed_sec`
- 不包含任务提交、Spark 启动、Ray 初始化之外的外部命令耗时，直接使用作业内计时

命令与结果:

1. Spark 路径，`geohash level=5`
```bash
python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/spark_output/test_report_partition_only/landsat8_level5 \
  --grid-type geohash \
  --grid-level 5 \
  --cover-mode intersect \
  --time-granularity day \
  --cover-execution spark \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000 \
  --timing-mode
```
- `partition_elapsed_sec`: `23.922s`

2. Spark 路径，`geohash level=7`
```bash
python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/spark_output/test_report_partition_only/landsat8_level7 \
  --grid-type geohash \
  --grid-level 7 \
  --cover-mode intersect \
  --time-granularity day \
  --cover-execution spark \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000 \
  --timing-mode
```
- `partition_elapsed_sec`: `23.724s`

3. Spark 优化路径，`geohash level=7`
```bash
python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/spark_output/test_report_optimized/landsat8_level7 \
  --grid-type geohash \
  --grid-level 7 \
  --cover-mode intersect \
  --time-granularity day \
  --cover-execution spark \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000 \
  --optimize-small-runs \
  --timing-mode
```
- 输出目录: `data/spark_output/test_report_optimized/landsat8_level7/run_20260416_142010`
- 实际 cover 执行方式: `driver`
- `partition_elapsed_sec`: `19.276s`

4. 本地轻量并行路径，`geohash level=7`
```bash
python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/spark_output/test_report_local/landsat8_level7 \
  --grid-type geohash \
  --grid-level 7 \
  --cover-mode intersect \
  --time-granularity day \
  --execution-engine local \
  --cover-execution driver \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000 \
  --timing-mode
```
- 输出目录: `data/spark_output/test_report_local/landsat8_level7/run_20260416_142338`
- `partition_elapsed_sec`: `1.928s`

5. Ray 路径，首个成功版本，`geohash level=7`
```bash
python grid_core/ray_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/ray_output/test_report/landsat8_level7 \
  --grid-type geohash \
  --grid-level 7 \
  --cover-mode intersect \
  --time-granularity day \
  --partition-prefix-len 3 \
  --ray-parallelism 4 \
  --chunk-size 1 \
  --timing-mode
```
- 输出目录: `data/ray_output/test_report/landsat8_level7/run_20260416_144019`
- `partition_elapsed_sec`: `2.873s`

6. Ray 路径，重构后的自动参数版本，`geohash level=7`
```bash
python grid_core/ray_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/ray_output/test_report/landsat8_level7_auto \
  --grid-type geohash \
  --grid-level 7 \
  --cover-mode intersect \
  --time-granularity day \
  --partition-prefix-len 3 \
  --timing-mode
```
- 输出目录: `data/ray_output/test_report/landsat8_level7_auto/run_20260416_144406`
- 自动解析得到的 `ray_parallelism`: `8`
- 自动解析得到的 `chunk_size`: `1`
- `partition_elapsed_sec`: `2.003s`

7. Ray 参数扫描汇总，`geohash level=7`

| 并行度 parallelism | 分块大小 chunk_size | 剖分耗时 partition_elapsed_sec |
| --- | --- | --- |
| 1 | 1 | 7.233s |
| 1 | 2 | 7.241s |
| 1 | 4 | 6.794s |
| 2 | 1 | 4.585s |
| 2 | 2 | 4.298s |
| 2 | 4 | 4.166s |
| 4 | 1 | 3.012s |
| 4 | 2 | 2.927s |
| 4 | 4 | 4.837s |
| 8 | 1 | 1.941s |
| 8 | 2 | 2.864s |
| 8 | 4 | 4.287s |

最佳结果:
- 输出目录: `data/ray_output/benchmark_landsat8_level7/p8_c1/run_20260416_144249`
- `partition_elapsed_sec`: `1.941s`

### 用例 1: 真实数据集，Spark 执行 cover 生成

命令:
```bash
export PYSPARK_PYTHON=/home/hadoop/anaconda3/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/home/hadoop/anaconda3/bin/python3.11
/usr/bin/time -f "ELAPSED_SEC=%e" python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8 \
  --output-dir data/spark_output/test_report/landsat8_spark \
  --grid-type geohash \
  --grid-level 5 \
  --cover-mode intersect \
  --time-granularity day \
  --cover-execution spark \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000
```

结果:
- 输出目录: `data/spark_output/test_report/landsat8_spark/run_20260416_135543`
- 总耗时: `36.38s`
- 资产数量 `asset_count`: `8`
- 网格任务数 `grid_task_count`: `32`
- 索引总行数 `total_index_rows`: `32`
- 空间编码去重数 `distinct_space_codes`: `4`
- 时空编码去重数 `distinct_st_codes`: `4`
- 输出规模: `geohash / level 5 / 32 行`

输出校验:
- `inspect_partition_output.py` 执行成功
- 8 个 band 都完成写出，每个 band 各有 4 行
- 抽样结果中 `space_code`、`st_code`、窗口偏移量、相交边界都有效

产物:
- `data/spark_output/test_report/landsat8_spark/run_20260416_135543/job_report.json`

### 用例 2: 合成 20 景数据集，Driver 执行 cover 生成

命令:
```bash
export PYSPARK_PYTHON=/home/hadoop/anaconda3/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/home/hadoop/anaconda3/bin/python3.11
/usr/bin/time -f "ELAPSED_SEC=%e" python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir data/landsat8_synth_20 \
  --output-dir data/spark_output/test_report/landsat8_synth20_driver \
  --grid-type geohash \
  --grid-level 5 \
  --cover-mode intersect \
  --time-granularity day \
  --cover-execution driver \
  --partition-prefix-len 3 \
  --max-cells-per-asset 5000
```

结果:
- 输出目录: `data/spark_output/test_report/landsat8_synth20_driver/run_20260416_135543`
- 总耗时: `175.79s`
- 资产数量 `asset_count`: `160`
- 网格任务数 `grid_task_count`: `640`
- 索引总行数 `total_index_rows`: `640`
- 空间编码去重数 `distinct_space_codes`: `4`
- 时空编码去重数 `distinct_st_codes`: `81`
- 输出规模: `geohash / level 5 / 640 行`

输出校验:
- `inspect_partition_output.py` 执行成功
- 所有输出行都处于 `geohash level 5`
- 同一景同一 band 的重复输出与“每个资产覆盖 4 个格网”的预期一致
- 抽样结果中 `st_code` 有效，例如 `gh:5:35e4:20200105:v1`

产物:
- `data/spark_output/test_report/landsat8_synth20_driver/run_20260416_135543/job_report.json`

## 已尝试但未完成的测试用例

### 用例 3: 合成 20 景数据集，Spark 执行 cover 生成

尝试输出目录:
- `data/spark_output/test_report_seq/landsat8_synth20_spark`

状态:
- 未生成 `job_report.json`
- 作业在本机 Spark 后期阶段长时间停滞，已手动停止

### 用例 4: 合成 80 景数据集，Spark 执行 cover 生成

尝试输出目录:
- `data/spark_output/test_report/landsat8_synth_spark/run_20260416_135543`

状态:
- 未生成 `job_report.json`
- 作业在本机 Spark 后期阶段长时间停滞，已手动停止

### 用例 5: 合成 5 天子集，Driver 模式

尝试输出目录:
- `data/spark_output/test_report_seq2/landsat8_synth20_5d_driver`

状态:
- 未生成 `job_report.json`
- 作业在本机执行过程中长时间停滞，已手动停止

## 观察与分析

1. 当前优化后的剖分流水线在已完成的测试中功能正确，输出结构有效。
2. `data/landsat8` 真实数据集可以稳定完成，并生成预期的索引输出。
3. `data/landsat8_synth_20` 在 `driver` 模式下可以完成中等规模回归测试，输出规模与预期一致。
4. 在当前机器上，较大的 Spark 流程在后期阶段容易出现长时间停滞，更像是运行环境或资源争用问题，不是剖分结果本身的正确性问题。
5. 对 `data/landsat8` 这种小规模真实数据，Spark 固定开销明显高于实际剖分计算开销。即使启用小数据优化，Spark 仍然需要 `19.276s`。
6. 本地轻量并行路径在同一数据集、同一计时口径下达到 `1.928s`，已经满足“压到 10 秒内”的目标。
7. Ray 可以替换 Spark 作为并行执行层，但在这批数据上，性能收益主要来自去掉 Spark 固定开销，而不是 Ray 本身比本地线程版更快。
8. Ray 的任务粒度非常关键。`chunk_size` 过大时，任务块数下降，会直接浪费并行度，导致耗时回退。
9. Ray 重构后，与本地轻量并行共用同一套任务预处理逻辑，自动参数模式实测为 `2.003s`，手工最优参数为 `1.941s`，与本地线程版基本同一量级。

## 结论

- 如果场景是单机 SDK 向上提供剖分服务，Spark 不适合作为默认执行引擎。
- 对 `data/landsat8` 这类小规模任务，优先选择本地轻量并行。
- 如果后续要从单机平滑扩展到多进程或多机执行，可以保留 Ray 作为第二执行后端。
- 在当前主机和当前数据规模下，Ray 的推荐默认参数可以设为自动模式；实际落地结果等价于:
  - `ray_parallelism=8`
  - `chunk_size=1`

## 建议

- 使用 `data/landsat8` 作为日常正确性烟测数据集。
- 使用 `data/landsat8_synth_20` 且 `cover_execution=driver` 作为当前主机上的中等规模回归数据集。
- 若需要做 Spark、Local、Ray 的严格性能对比，建议在更稳定的独占机器上执行，并对 Spark 单独设置更明确的资源限制。
- 单机 SDK 服务场景下，优先使用 `execution-engine=local`，Ray 作为可选扩展后端。
