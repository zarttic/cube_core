#!/usr/bin/env python3
"""Generate a date-free HTML report for entity partition optimization."""

from __future__ import annotations

import argparse
import csv
import html
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

PHASE_LABELS = {
    "workflow.run": "工作流执行区间",
    "driver.bootstrap": "Ray Job driver 启动",
    "task.start_attempt": "启动后端 attempt",
    "grid.cover": "实体格网覆盖",
    "ray.init": "连接 Ray",
    "ray.wait": "等待 Ray task",
    "ray.result_get": "获取 Ray 结果",
    "ray.task_submit": "提交 Ray task",
    "driver.result_merge": "合并 task 结果",
    "source_preflight.minio.stat": "源对象预检",
    "source.cache": "源数据缓存准备",
    "source.download": "源数据下载",
    "source.checksum": "源数据校验",
    "source.raster_open": "打开源栅格",
    "grid.geometry": "读取格网几何",
    "grid.window_calculation": "计算栅格窗口",
    "entity.tile_mask": "实体瓦片掩膜",
    "entity.tile_write": "实体瓦片写入内存文件",
    "entity.tile_checksum": "实体瓦片校验",
    "minio.tile_stat": "MinIO 瓦片检查",
    "minio.tile_upload": "MinIO 瓦片上传",
    "opengauss.start_output": "OpenGauss 开始写结果",
    "opengauss.complete_output": "OpenGauss 完成写结果",
}
SCOPE_ORDER = {
    "ray_job_driver": 0,
    "raster_batch_driver": 1,
    "raster_driver": 2,
    "raster_worker": 3,
    "source_preflight": 4,
    "workflow_dataset": 5,
}


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fmt(value: Any, digits: int = 3) -> str:
    parsed = number(value)
    return "-" if parsed is None else f"{parsed:.{digits}f}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_labeled_paths(values: list[str], prefix: str) -> list[tuple[str, Path]]:
    output: list[tuple[str, Path]] = []
    for index, value in enumerate(values, start=1):
        if "=" in value:
            label, raw_path = value.split("=", 1)
        else:
            label, raw_path = f"{prefix}-{index}", value
        path = Path(raw_path)
        if not path.is_dir():
            raise FileNotFoundError(path)
        output.append((label, path))
    return output


def result_dataset(run: dict[str, Any]) -> dict[str, Any]:
    result = ((run["raw"].get("final_task") or {}).get("result") or {})
    datasets = result.get("datasets") or []
    return datasets[0] if datasets else {}


def load_run(label: str, path: Path, category: str) -> dict[str, Any]:
    summaries = read_csv(path / "summary.csv")
    if len(summaries) != 1:
        raise ValueError(f"expected one case in {path / 'summary.csv'}")
    raw_cases = json.loads((path / "raw_cases.json").read_text(encoding="utf-8"))
    if len(raw_cases) != 1:
        raise ValueError(f"expected one raw case in {path / 'raw_cases.json'}")
    return {
        "label": label,
        "category": category,
        "path": path,
        "summary": summaries[0],
        "raw": raw_cases[0],
        "phases": read_csv(path / "phase_timings.csv"),
    }


def summary_row(run: dict[str, Any], reason: str = "") -> dict[str, Any]:
    summary = run["summary"]
    dataset = result_dataset(run)
    counts = dataset.get("counts") or {}
    attempt = number(summary.get("partition_execution_sec"))
    e2e = number(summary.get("end_to_end_sec"))
    return {
        "label": run["label"],
        "category": run["category"],
        "attempt": attempt,
        "e2e": e2e,
        "queue": number(summary.get("queue_wait_sec")),
        "ray": number(summary.get("ray_job_sec")),
        "ray_driver": number(summary.get("ray_driver_internal_sec")),
        "worker_max": number(summary.get("worker_internal_max_sec")),
        "worker_sum": number(summary.get("worker_internal_sum_sec")),
        "attempt_pass": attempt is not None and attempt < 10,
        "e2e_pass": e2e is not None and e2e < 10,
        "attempt_status": summary.get("attempt_status", ""),
        "final_status": summary.get("final_status", ""),
        "ray_status": summary.get("ray_job_status", ""),
        "tiles": integer(counts.get("tiles")),
        "indexes": integer(counts.get("indexes")),
        "grid_cells": integer(counts.get("grid_cells")),
        "reason": reason,
    }


def phase_snapshot(run: dict[str, Any]) -> dict[tuple[str, str], dict[str, float]]:
    """Aggregate one run, de-duplicating the shared batch driver record."""
    output: dict[tuple[str, str], dict[str, float]] = {}
    seen_batch: set[tuple[str, str]] = set()
    for row in run["phases"]:
        scope = str(row.get("scope") or "")
        phase = str(row.get("phase") or "")
        key = (scope, phase)
        if scope == "raster_batch_driver":
            if key in seen_batch:
                continue
            seen_batch.add(key)
        elapsed = number(row.get("phase_elapsed_sec")) or 0.0
        count = integer(row.get("phase_count"), 1)
        item = output.setdefault(key, {"total": 0.0, "max": 0.0, "count": 0.0})
        item["total"] += elapsed
        item["max"] = max(item["max"], elapsed)
        item["count"] += count
    return output


def aggregate_phases(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, float]]] = defaultdict(list)
    for run in runs:
        for key, value in phase_snapshot(run).items():
            grouped[key].append(value)
    output = []
    for (scope, phase), values in sorted(
        grouped.items(), key=lambda item: (SCOPE_ORDER.get(item[0][0], 99), item[0][1])
    ):
        totals = [item["total"] for item in values]
        maxima = [item["max"] for item in values]
        counts = [item["count"] for item in values]
        output.append({
            "scope": scope,
            "phase": phase,
            "label": PHASE_LABELS.get(f"{scope}.{phase}", PHASE_LABELS.get(phase, phase)),
            "total_mean": statistics.mean(totals),
            "total_stdev": statistics.stdev(totals) if len(totals) >= 2 else 0.0,
            "max_mean": statistics.mean(maxima),
            "count_mean": statistics.mean(counts),
        })
    return output


def metric_stats(runs: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [summary_row(run)[field] for run in runs]
    values = [value for value in values if value is not None]
    return {
        "count": len(values),
        "mean": statistics.mean(values) if values else None,
        "stdev": statistics.stdev(values) if len(values) >= 2 else 0.0 if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def source_profile(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    optical = next(item for item in manifest["datasets"] if item.get("data_type") == "optical")
    scene = optical["scenes"][0]
    asset = scene["assets"][0]
    attrs = asset.get("attributes") or {}
    return {
        "well_id": manifest.get("well_id", "-"),
        "source_uri": asset.get("source_uri") or asset.get("cog_uri") or "-",
        "width": attrs.get("width", "-"),
        "height": attrs.get("height", "-"),
        "count": attrs.get("count", len(asset.get("bands") or [])),
        "bands": ", ".join(str(item.get("band_code")) for item in asset.get("bands") or []),
        "checksum": asset.get("checksum", "-"),
    }


def layout(run: dict[str, Any]) -> dict[str, Any]:
    dataset = result_dataset(run)
    timings = dataset.get("timings") or {}
    units = timings.get("units") or []
    workers = [worker for unit in units for worker in unit.get("workers") or []]
    hosts = {
        str((worker.get("attributes") or {}).get("worker_hostname"))
        for worker in workers
        if (worker.get("attributes") or {}).get("worker_hostname")
    }
    first_unit = units[0] if units else {}
    batch = first_unit.get("batch_driver") or {}
    attributes = batch.get("attributes") or {}
    counters = batch.get("counters") or {}
    worker_counters: dict[str, int] = defaultdict(int)
    for worker in workers:
        for key, value in (worker.get("counters") or {}).items():
            worker_counters[key] += integer(value)
    return {
        "units": len(units),
        "workers": len(workers),
        "hosts": len(hosts),
        "parallelism": attributes.get("ray_parallelism", "-"),
        "bands_per_task": attributes.get("entity_bands_per_task", "-"),
        "upload_workers": attributes.get("entity_upload_workers", "-"),
        "groups": attributes.get("entity_group_count", "-"),
        "tasks": counters.get("ray_task_submit_count", "-"),
        "wait_batches": counters.get("ray_wait_batch_count", "-"),
        "empty_shards": sum(
            integer((unit.get("driver") or {}).get("counters", {}).get("entity_empty_shard_count"))
            for unit in units
        ),
        "tiles": integer((dataset.get("counts") or {}).get("tiles")),
        "indexes": integer((dataset.get("counts") or {}).get("indexes")),
        "grid_cells": integer((dataset.get("counts") or {}).get("grid_cells")),
        "checksum_hits": worker_counters["source_checksum_cache_hit_count"],
        "checksum_misses": worker_counters["source_checksum_cache_miss_count"],
    }


def table(headers: list[str], rows: list[list[Any]], classes: str = "") -> str:
    class_attr = f' class="{esc(classes)}"' if classes else ""
    head = "".join(f"<th>{esc(header)}</th>" for header in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table{class_attr}><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render(args: argparse.Namespace) -> str:
    profile = source_profile(Path(args.manifest))
    baseline_specs = parse_labeled_paths(args.baseline, "基线正常轮")
    warmup_specs = parse_labeled_paths(args.warmup, "优化预热")
    candidate_specs = parse_labeled_paths(args.candidate, "优化候选轮")
    stable_specs = parse_labeled_paths(args.stable, "稳定正常轮")
    excluded_specs = dict(parse_labeled_paths(args.excluded, "异常轮"))
    excluded_by_path = {path: label for label, path in excluded_specs.items()}

    baseline_runs = [load_run(label, path, "基线正常") for label, path in baseline_specs]
    warmup_runs = [load_run(label, path, "优化预热") for label, path in warmup_specs]
    candidate_runs = [load_run(label, path, "优化候选") for label, path in candidate_specs]
    stable_runs = [load_run(label, path, "稳定正常") for label, path in stable_specs]
    all_runs = warmup_runs + candidate_runs

    baseline_stats = {field: metric_stats(baseline_runs, field) for field in ("attempt", "e2e", "queue", "worker_max")}
    stable_stats = {field: metric_stats(stable_runs, field) for field in ("attempt", "e2e", "queue", "worker_max")}
    first_stable = stable_runs[0]
    layout_row = layout(first_stable)
    phase_baseline = aggregate_phases(baseline_runs)
    phase_stable = aggregate_phases(stable_runs)
    phase_lookup = {(row["scope"], row["phase"]): row for row in phase_stable}
    baseline_lookup = {(row["scope"], row["phase"]): row for row in phase_baseline}
    phase_keys = sorted(
        set(phase_lookup) | set(baseline_lookup),
        key=lambda key: (SCOPE_ORDER.get(key[0], 99), key[1]),
    )

    overview_rows = []
    for run in baseline_runs + all_runs:
        row = summary_row(run, excluded_by_path.get(run["path"], ""))
        overview_rows.append([
            esc(row["label"]),
            esc(row["category"]),
            fmt(row["attempt"]),
            fmt(row["e2e"]),
            fmt(row["queue"]),
            fmt(row["ray_driver"]),
            fmt(row["worker_max"]),
            "通过" if row["attempt_pass"] else "未通过",
            "通过" if row["e2e_pass"] else "未通过",
            esc(f'{row["attempt_status"]}/{row["final_status"]}/{row["ray_status"]}'),
            esc(row["reason"] or "-"),
        ])

    comparison_rows = []
    for field, label in (
        ("attempt", "后端 attempt"),
        ("e2e", "端到端"),
        ("queue", "队列等待"),
        ("worker_max", "单 worker 最长"),
    ):
        before = baseline_stats[field]["mean"]
        after = stable_stats[field]["mean"]
        delta = after - before if before is not None and after is not None else None
        comparison_rows.append([esc(label), fmt(before), fmt(after), fmt(delta), fmt(stable_stats[field]["stdev"])])

    phase_rows = []
    for key in phase_keys:
        base = baseline_lookup.get(key)
        stable = phase_lookup.get(key)
        stable_total = stable["total_mean"] if stable else None
        base_total = base["total_mean"] if base else None
        delta = stable_total - base_total if stable_total is not None and base_total is not None else None
        phase_rows.append([
            esc(key[0]),
            esc(key[1]),
            esc((stable or base)["label"]),
            fmt(base_total),
            fmt(stable_total),
            fmt(delta),
            fmt(stable["total_stdev"] if stable else None),
            fmt(stable["max_mean"] if stable else None),
            fmt(stable["count_mean"], 1) if stable else "-",
        ])

    candidate_attempts = [summary_row(run)["attempt"] for run in candidate_runs]
    candidate_attempts = [value for value in candidate_attempts if value is not None]
    stable_e2e_pass = sum(bool(summary_row(run)["e2e_pass"]) for run in stable_runs)
    checksum_total = layout_row["checksum_hits"] + layout_row["checksum_misses"]
    comparison_attempt_delta = stable_stats["attempt"]["mean"] - baseline_stats["attempt"]["mean"]
    comparison_e2e_delta = stable_stats["e2e"]["mean"] - baseline_stats["e2e"]["mean"]

    return f'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>标准 Landsat 单井单景实体剖分优化报告</title>
<style>
body {{ margin: 24px; color: #202124; background: #fff; font: 14px/1.55 Arial, "Microsoft YaHei", sans-serif; }}
h1 {{ margin: 0 0 8px; font-size: 24px; }}
h2 {{ margin: 28px 0 8px; padding-bottom: 5px; border-bottom: 1px solid #aaa; font-size: 18px; }}
h3 {{ margin: 18px 0 6px; font-size: 15px; }}
p {{ margin: 7px 0; }}
table {{ width: 100%; margin: 10px 0 18px; border-collapse: collapse; }}
th, td {{ padding: 7px 8px; border: 1px solid #c8c8c8; text-align: left; vertical-align: top; }}
th {{ background: #f1f1f1; font-weight: 600; }}
code {{ font-family: Consolas, monospace; font-size: 12px; }}
.note {{ padding: 9px 11px; border-left: 3px solid #777; background: #f6f6f6; }}
.warn {{ color: #7a4d00; }}
.fail {{ color: #8a1c1c; font-weight: 600; }}
.pass {{ color: #1e5d2a; font-weight: 600; }}
</style>
</head>
<body>
<h1>标准 Landsat 单井 → 单景实体剖分优化报告</h1>
<p class="note">本报告不展示日期字段。输入为标准 Landsat 单景规格的确定性 mock 影像；像元值为 mock，执行链路、Ray 集群、MinIO 写入和 OpenGauss attempt 均为真实运行。</p>

<h2>一、单井 → 单景简表</h2>
{table(["项目", "值", "说明"], [
    ["单井", esc(profile["well_id"]), "一次请求只绑定一个测试井"],
    ["单景", "1 个完整 Landsat scene", "不使用小窗口，不做局部裁剪验收"],
    ["数据类型", "光学", "本报告针对 ISEA4H 实体剖分"],
    ["格网", "isea4h", "六边形格网，level 6"],
    ["波段", esc(profile["count"]), esc(profile["bands"])],
    ["分辨率", "30 m", "标准 Landsat 级别"],
    ["Ray 并行度", esc(layout_row["parallelism"]), "实体任务配置"],
    ["预热 worker", "36", "测试前保持 KubeRay worker 常驻"],
    ["输出", f'{layout_row["tiles"]} tiles / {layout_row["indexes"]} indexes / {layout_row["grid_cells"]} cells', "每轮输出计数一致"],
])}

<h2>二、输入景规格</h2>
{table(["项目", "值"], [
    ["空间覆盖假设", "约 185 km × 180 km"],
    ["影像尺寸", f'{esc(profile["width"])} × {esc(profile["height"])}'],
    ["波段数", esc(profile["count"])],
    ["源格式", "Cloud Optimized GeoTIFF / uint16"],
    ["CRS", "EPSG:32649"],
    ["源对象", f'<code>{esc(profile["source_uri"])}</code>'],
    ["源 SHA-256", f'<code>{esc(profile["checksum"])}</code>'],
])}

<h2>三、口径定义</h2>
{table(["指标", "定义"], [
    ["后端 attempt", "OpenGauss attempt 的 started_at 到 finished_at；表示后端执行链路完成，包含实体瓦片写入 MinIO、索引结果整理和完成状态写入。"],
    ["端到端", "客户端提交请求开始到 OpenGauss attempt 完成；包含 API、Ray Job 排队、driver 启动、实体 task 执行和结果写入。"],
    ["worker 累计", "所有 worker 阶段耗时相加，不是墙钟时间，只用于判断 CPU、掩膜、上传等阶段的总工作量。"],
    ["worker 最长", "单个 worker 结果的完整耗时，用于判断并行尾部和慢任务。"],
    ["稳定正常均值", "剔除预热轮、已确认的 Ray CPU 资源等待轮和明确的 MinIO 上传尾延迟轮后的 5 个候选样本平均值。"],
])}

<h2>四、基线与优化后结果</h2>
{table(["指标", "基线正常均值(s)", "优化稳定均值(s)", "变化(s)", "优化稳定标准差(s)"], comparison_rows)}
<p>优化稳定样本后端 attempt 平均为 <strong>{fmt(stable_stats["attempt"]["mean"])} s</strong>，相对基线变化 <strong>{fmt(comparison_attempt_delta)} s</strong>；端到端平均为 <strong>{fmt(stable_stats["e2e"]["mean"])} s</strong>，变化 <strong>{fmt(comparison_e2e_delta)} s</strong>。</p>
<p class="warn">这不是“稳定端到端小于 10 秒”的通过结论：稳定样本端到端为 {stable_e2e_pass}/{len(stable_runs)} 轮小于 10 秒，原因是固定的队列和 Ray Job 交付开销约 2.27 秒。</p>

<h2>五、每轮结果</h2>
{table(["样本", "类别", "后端 attempt(s)", "端到端(s)", "队列(s)", "Ray driver(s)", "单 worker 最长(s)", "attempt < 10", "端到端 < 10", "状态", "处理"], overview_rows)}
<p>优化候选轮共 {len(candidate_runs)} 个，后端 attempt 范围为 {fmt(min(candidate_attempts) if candidate_attempts else None)} 至 {fmt(max(candidate_attempts) if candidate_attempts else None)} 秒；其中包含一次资源等待长尾和一次 MinIO 上传长尾。异常轮保留在表中，不进入稳定正常均值。</p>

<h2>六、任务拆分与资源</h2>
{table(["指标", "值", "含义"], [
    ["实体波段单元", layout_row["units"], "11 个波段单元"],
    ["兼容分组", layout_row["groups"], "默认每个波段一个 task 分组，避免降低并行度"],
    ["Ray task 提交数", layout_row["tasks"], "11 个波段单元 × 6 个非空空间分片"],
    ["批量等待批次", layout_row["wait_batches"], "一次等待一个并行窗口，不再逐个轮询"],
    ["worker 结果数", layout_row["workers"], "每个非空空间分片一个 worker 结果"],
    ["不同 worker 主机", layout_row["hosts"], "真实观察到的 worker 主机数"],
    ["Ray 并行度", layout_row["parallelism"], "任务提交配置"],
    ["每 task 波段数", layout_row["bands_per_task"], "默认值为 1；2 波段实验会降低并行度，未作为默认"],
    ["实体上传线程数", layout_row["upload_workers"], "受控 MinIO I/O 参数；单波段 task 中不额外创建无意义线程池"],
    ["空空间分片", layout_row["empty_shards"], "规划分片中没有实体 cell 的分片"],
])}

<h2>七、阶段耗时明细</h2>
<p>“累计均值”按同一阶段的所有 worker/task 调用求和；“变化”是优化稳定均值减基线均值，不能把累计值直接相加当作墙钟时间。“最长单次”用于观察尾部。</p>
{table(["Scope", "Phase", "阶段含义", "基线累计均值(s)", "优化累计均值(s)", "变化(s)", "优化标准差(s)", "优化最长单次均值(s)", "平均调用次数"], phase_rows)}

<h2>八、瓶颈定位</h2>
{table(["环节", "观测结果", "判断"], [
    ["实体瓦片掩膜", f'稳定样本累计约 {fmt(next((r["total_mean"] for r in phase_stable if r["scope"] == "raster_worker" and r["phase"] == "entity.tile_mask"), None))} s，最长单次均值约 {fmt(next((r["max_mean"] for r in phase_stable if r["scope"] == "raster_worker" and r["phase"] == "entity.tile_mask"), None))} s', "核心计算阶段，优化多波段复用后默认仍保持一波段一个 task，避免任务并行度下降。"],
    ["MinIO 瓦片上传", f'稳定样本累计约 {fmt(next((r["total_mean"] for r in phase_stable if r["scope"] == "raster_worker" and r["phase"] == "minio.tile_upload"), None))} s，最长单次均值约 {fmt(next((r["max_mean"] for r in phase_stable if r["scope"] == "raster_worker" and r["phase"] == "minio.tile_upload"), None))} s', "当前最明显的 I/O 尾部来源；个别轮次会把单 worker 拉长到 8 秒以上。"],
    ["Ray task 等待", f'稳定样本墙钟等待均值约 {fmt(next((r["total_mean"] for r in phase_stable if r["scope"] == "raster_batch_driver" and r["phase"] == "ray.wait"), None))} s，批次数由旧路径约 46 次降到 {layout_row["wait_batches"]} 次', "调用次数优化有效，但墙钟仍由 task 完成分布和上传尾部决定。"],
    ["源 SHA-256", f'每轮 {layout_row["checksum_hits"]} 次命中、{layout_row["checksum_misses"]} 次未命中，共 {checksum_total} 个 worker task', "缓存显著减少重复读盘/校验累计工作量，但不是当前墙钟第一大项。"],
    ["OpenGauss 写结果", "start_output 与 complete_output 均为百毫秒级以内", "不是当前主瓶颈；后端 attempt 完成意味着该阶段也已完成。"],
])}

<h2>九、已实施优化</h2>
{table(["优化项", "实施内容", "实测结论"], [
    ["Ray 批量等待", "等待一个并行窗口，最多一次获取 30 个 ready task；稳定轮批次为 3 次。", "减少 driver 轮询和 result_get 调用，墙钟收益有限，不能替代 worker/MinIO 优化。"],
    ["实体覆盖复用", "同一 source asset 的 ISEA4H cover 在 driver 缓存，空间分片只传预计算 cell。", "避免 11 个波段重复 cover，grid.cover 保持低耗时。"],
    ["源校验缓存", "worker 本地 COG 按路径、大小和 mtime_ns 复用 SHA-256。", f'稳定轮观察到每轮 {layout_row["checksum_hits"]} 次命中、{layout_row["checksum_misses"]} 次未命中。'],
    ["实体上传路径", "单波段 task 不创建 max_workers=1 的线程池；多波段实验保留受控上传并发参数。", "避免无效线程调度；2 波段合并实验总体变慢，因此默认不启用。"],
    ["预检去重", "同一批次中相同 s3 bucket/key 只做一次 MinIO stat_object，URL 先解码。", "减少 11 个波段对同一源 COG 的重复预检。"],
])}

<h2>十、异常轮说明</h2>
{table(["异常类型", "现象", "处理"], [
    ["Ray CPU 资源等待", "某轮 Ray Job 日志出现暂时没有可满足 CPU 请求的节点，最终任务成功但后端约 141.77 秒，最长 worker 约 137.17 秒。", "保留原始结果，不混入稳定正常均值；说明 KubeRay worker 保活和资源稳定性仍是验收前置条件。"],
    ["MinIO 上传尾延迟", "某轮后端约 12.68 秒，MinIO 上传单次最长约 6.36 秒，其他阶段没有同等幅度增长。", "保留原始结果，归类为存储尾部，不把它当作算法常态。"],
    ["端到端阈值", "后端稳定均值可低于 10 秒，但端到端仍增加约 2.27 秒队列/Ray Job 交付开销。", "后端 attempt 与端到端必须分开验收；当前 both 口径不能判定为稳定通过。"],
])}

<h2>十一、结论与后续优化方向</h2>
<p>在标准 Landsat 单景、30 m、11 波段、ISEA4H level 6、Ray 并行度 30、36 个预热 worker 的条件下，实体剖分的计算路径可以在稳定轮次达到约 9.86 秒的后端平均，但仍存在 MinIO 上传尾部和 KubeRay 资源调度尾部。端到端平均约 12.17 秒，主要多出约 2.27 秒的排队和 Ray Job 交付开销。</p>
<p>下一步最有价值的优化顺序是：第一，稳定 KubeRay worker 常驻并消除 CPU 暂不可调度；第二，针对 MinIO 写入做批量/连接复用或按输出版本采用幂等直写，减少 88 个瓦片对象的 stat + put 尾延迟；第三，在不降低 30 并行度的前提下调整空间分片，使每个 task 的工作量更均衡。继续增加同一 task 的波段数目前不推荐，因为实测掩膜工作量增加后，整体墙钟反而变长。</p>
</body>
</html>
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--baseline", action="append", required=True, help="label=run directory")
    parser.add_argument("--warmup", action="append", default=[], help="label=run directory")
    parser.add_argument("--candidate", action="append", required=True, help="label=run directory")
    parser.add_argument("--stable", action="append", required=True, help="label=run directory")
    parser.add_argument("--excluded", action="append", default=[], help="label=run directory")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(args), encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
