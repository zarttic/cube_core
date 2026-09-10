# 归档脚本

归档时间：2026-09-10

本目录保存一次性或已被取代的脚本：性能测试与报告生成、样例数据准备、早期 Ray 集群启停与
端到端脚本。它们不随当前代码维护，使用时需要自行确认环境依赖。

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| `cube_web/generate_entity_optimization_report.py` | `cube_web/scripts/` | 实体剖分优化报告生成 |
| `cube_web/generate_landsat_mock_report.py` | `cube_web/scripts/` | Landsat mock 报告生成 |
| `cube_web/generate_resource_tuning_report.py` | `cube_web/scripts/` | 资源调优报告生成 |
| `cube_web/prepare_mock_landsat_scene.py` | `cube_web/scripts/` | mock Landsat 样例准备 |
| `cube_web/prepare_sentinel10_scene.py` | `cube_web/scripts/` | Sentinel-1/2 样例准备 |
| `cube_split/start_ray_head.sh`、`start_ray_worker.sh`、`stop_ray_cluster.sh` | `cube_split/scripts/` | 早期本地 Ray 集群启停 |
| `cube_split/install_cube_encoder_pkg.sh` | `cube_split/scripts/` | 早期 encoder 包安装脚本 |
| `cube_split/run_distributed_partition_test.sh` | `cube_split/scripts/` | 早期分布式剖分测试脚本 |
| `cube_split/run_ray_ingest_e2e.sh` | `cube_split/scripts/` | 光学入库端到端脚本（需外部 Ray/MinIO/OpenGauss） |
| `update_test_doc.py` | `scripts/` | 早期测试文档更新脚本 |

仍在维护的脚本留在原位置：`cube_web/scripts/` 的 `migrate_scene_domain.py`、`reset_partition_domain.py`、
`backfill_partition_tile_publication.py` 以及有测试覆盖的性能脚本；`cube_split/scripts/run_logical_partition_benchmark.py`。
