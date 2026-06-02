# 剖分演示环境

本目录只保存演示专用的运行说明和示例运行时配置。生产剖分代码仍在 `cube_split`
和 `cube_web` 中维护；演示数据、seed 批次和集群冒烟编排必须通过运行时配置显式启用。

## 启用演示 Seed 批次

生产启动默认不加载内置演示剖分批次。只有演示环境才设置：

```bash
CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1
```

内置 seed 批次会引用本地或 MinIO 上的演示资产，例如光学、产品、雷达和碳卫星样例。
这些批次不应在生产数据库中自动创建。

## 运行演示冒烟

使用本地 `.cube_web.env` 中的 PostgreSQL、Ray 和 MinIO 配置后运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python cube_split/scripts/run_all_partition_flows_smoke.py --mode demo
```

更轻量、不入库的检查使用：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python cube_split/scripts/run_all_partition_flows_smoke.py --mode test
```
