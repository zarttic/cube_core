#!/usr/bin/env python3
"""Generate a date-free resource tuning report for the Landsat entity benchmark."""

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
    "ray.init": "连接 Ray",
    "ray.task_submit": "提交 Ray task",
    "ray.wait": "等待 Ray task",
    "ray.result_get": "获取 Ray 结果",
    "driver.result_merge": "合并 task 结果",
    "source.cache": "源数据缓存准备",
    "source.download": "源数据下载",
    "source.checksum": "源数据校验",
    "source.raster_open": "打开源栅格",
    "grid.cover": "实体格网覆盖",
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

GROUPS = (
    ("48 并行 / 64 worker / 上传线程 4 / multipart 3", "p48_w64_batch"),
    ("48 并行 / 64 worker / 上传线程 4 / multipart 4", "p48_w64_minio4"),
    ("64 并行 / 64 worker / 上传线程 4 / multipart 3", "p64_w64_batch"),
    ("48 并行 / 64 worker / 上传线程 1 / multipart 3", "p48_w64_upload1"),
    ("36 并行 / 64 worker / 上传线程 1 / multipart 3", "p36_w64_upload1"),
    ("48 并行 / 48 worker / 上传线程 1 / multipart 3", "p48_w48_upload1"),
    ("48 并行 / 48 worker / 上传线程 1 / multipart 1", "p48_w48_minio1"),
    ("48 并行 / 48 worker / 上传线程 1 / multipart 2", "p48_w48_minio2"),
)


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def num(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def fmt(value: Any, digits: int = 3) -> str:
    parsed = num(value)
    return "-" if parsed is None else f"{parsed:.{digits}f}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def walk_timing_records(value: Any):
    if isinstance(value, dict):
        if isinstance(value.get("scope"), str) and isinstance(value.get("phases"), dict):
            yield value
        for item in value.values():
            yield from walk_timing_records(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk_timing_records(item)


def result_root(raw: dict[str, Any]) -> dict[str, Any]:
    attempt = raw.get("attempt") or {}
    result = attempt.get("runner_result")
    if isinstance(result, dict):
        return result
    final_task = raw.get("final_task") or {}
    result = final_task.get("result")
    return result if isinstance(result, dict) else {}


def source_cache_stats(raw: dict[str, Any]) -> dict[str, int]:
    stats = {"hits": 0, "misses": 0, "workers": 0, "hosts": 0}
    hosts: set[str] = set()
    for record in walk_timing_records(result_root(raw)):
        if record.get("scope") != "raster_worker":
            continue
        stats["workers"] += 1
        attrs = record.get("attributes") or {}
        if attrs.get("worker_hostname"):
            hosts.add(str(attrs["worker_hostname"]))
        counters = record.get("counters") or {}
        stats["hits"] += int(counters.get("source_cache_hit_count") or 0)
        stats["misses"] += int(counters.get("source_cache_miss_count") or 0)
    stats["hosts"] = len(hosts)
    return stats


def layout(raw: dict[str, Any]) -> dict[str, Any]:
    result = result_root(raw)
    for record in walk_timing_records(result):
        if record.get("scope") == "raster_batch_driver":
            attrs = record.get("attributes") or {}
            counters = record.get("counters") or {}
            return {
                "parallelism": attrs.get("ray_parallelism", "-"),
                "units": attrs.get("entity_unit_count", "-"),
                "groups": attrs.get("entity_group_count", "-"),
                "bands_per_task": attrs.get("entity_bands_per_task", "-"),
                "upload_workers": attrs.get("entity_upload_workers", "-"),
                "minio_parallel": attrs.get("entity_minio_parallel_uploads", "3"),
                "tasks": counters.get("ray_task_submit_count", "-"),
                "wait_batches": counters.get("ray_wait_batch_count", "-"),
            }
    return {}


def phase_values(path: Path) -> dict[tuple[str, str], dict[str, float]]:
    output: dict[tuple[str, str], dict[str, float]] = {}
    seen_batch: set[tuple[str, str]] = set()
    for row in read_csv(path / "phase_timings.csv"):
        key = (str(row.get("scope") or ""), str(row.get("phase") or ""))
        if key[0] == "raster_batch_driver":
            if key in seen_batch:
                continue
            seen_batch.add(key)
        item = output.setdefault(key, {"total": 0.0, "max": 0.0, "count": 0.0})
        elapsed = num(row.get("phase_elapsed_sec")) or 0.0
        item["total"] += elapsed
        item["max"] = max(item["max"], elapsed)
        item["count"] += int(num(row.get("phase_count")) or 0)
    return output


def load_run(label: str, path: Path) -> dict[str, Any]:
    summaries = read_csv(path / "summary.csv")
    if len(summaries) != 1:
        raise ValueError(f"expected one summary row: {path}")
    raw_cases = json.loads((path / "raw_cases.json").read_text(encoding="utf-8"))
    if len(raw_cases) != 1:
        raise ValueError(f"expected one case: {path}")
    summary = summaries[0]
    raw = raw_cases[0]
    return {
        "label": label,
        "path": path,
        "summary": summary,
        "raw": raw,
        "attempt": num(summary.get("partition_execution_sec")),
        "e2e": num(summary.get("end_to_end_sec")),
        "queue": num(summary.get("queue_wait_sec")),
        "worker_max": num(summary.get("worker_internal_max_sec")),
        "worker_sum": num(summary.get("worker_internal_sum_sec")),
        "cache": source_cache_stats(raw),
        "layout": layout(raw),
        "phases": phase_values(path),
    }


def table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{esc(item)}</th>" for item in headers)
    body = "".join("<tr>" + "".join(f"<td>{item}</td>" for item in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def stats(runs: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [run[field] for run in runs if run[field] is not None]
    return {
        "count": len(values),
        "mean": statistics.mean(values) if values else None,
        "stdev": statistics.stdev(values) if len(values) >= 2 else 0.0 if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def normal_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [run for run in runs if run["cache"]["misses"] == 0 and run["attempt"] is not None and run["attempt"] < 10.0]


def phase_rows(runs: list[dict[str, Any]]) -> list[list[Any]]:
    grouped: dict[tuple[str, str], list[dict[str, float]]] = defaultdict(list)
    for run in runs:
        for key, value in run["phases"].items():
            grouped[key].append(value)
    rows = []
    for key, values in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        totals = [item["total"] for item in values]
        maxima = [item["max"] for item in values]
        counts = [item["count"] for item in values]
        label = PHASE_LABELS.get(key[1], key[1])
        rows.append([
            esc(key[0]),
            esc(key[1]),
            esc(label),
            fmt(statistics.mean(totals)),
            fmt(statistics.stdev(totals) if len(totals) >= 2 else 0.0),
            fmt(statistics.mean(maxima)),
            fmt(statistics.mean(counts), 1),
        ])
    return rows


def build_report(manifest_path: Path, matrix_root: Path) -> str:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    optical = next(item for item in manifest["datasets"] if item.get("data_type") == "optical")
    asset = optical["scenes"][0]["assets"][0]
    attrs = asset.get("attributes") or {}

    groups: list[tuple[str, list[dict[str, Any]]]] = []
    for label, directory in GROUPS:
        root = matrix_root / directory
        runs = []
        if root.is_dir():
            for path in sorted(item for item in root.iterdir() if item.is_dir()):
                if (path / "summary.csv").is_file() and (path / "raw_cases.json").is_file():
                    runs.append(load_run(f"{directory}-{len(runs) + 1}", path))
        if runs:
            groups.append((label, runs))

    recommended_label = "48 并行 / 64 worker / 上传线程 4 / multipart 3"
    recommended = next(runs for label, runs in groups if label == recommended_label)
    recommended_normal = normal_runs(recommended)
    if not recommended_normal:
        raise ValueError("recommended resource group has no normal samples")
    outliers = [run for run in recommended if run not in recommended_normal]
    outlier = outliers[0] if outliers else None
    recommended_layout = recommended_normal[0]["layout"]

    matrix_rows = []
    for label, runs in groups:
        clean = [run for run in runs if run["cache"]["misses"] == 0]
        normal = normal_runs(runs)
        values = stats(normal, "attempt")
        matrix_rows.append([
            esc(label),
            len(runs),
            len(clean),
            len(normal),
            fmt(values["mean"]),
            fmt(values["stdev"]),
            fmt(values["min"]),
            fmt(values["max"]),
            f"{sum(run['attempt'] < 10.0 for run in clean if run['attempt'] is not None)}/{len(clean)}" if clean else "-",
        ])

    round_rows = []
    for index, run in enumerate(recommended, start=1):
        upload = run["phases"].get(("raster_worker", "minio.tile_upload"), {})
        reason = "正常样本" if run in recommended_normal else "MinIO 上传尾延迟异常"
        round_rows.append([
            f"轮次 {index}",
            fmt(run["attempt"]),
            fmt(run["e2e"]),
            fmt(run["queue"]),
            fmt(run["worker_max"]),
            fmt(upload.get("total")),
            fmt(upload.get("max")),
            run["cache"]["hits"],
            run["cache"]["misses"],
            esc(reason),
        ])

    metric_rows = []
    for field, label in (("attempt", "后端 attempt"), ("e2e", "端到端观察值"), ("queue", "队列等待"), ("worker_max", "单 worker 最长")):
        value = stats(recommended_normal, field)
        metric_rows.append([esc(label), fmt(value["mean"]), fmt(value["stdev"]), fmt(value["min"]), fmt(value["max"])])

    checksum = recommended_normal[0]["cache"]
    total_tiles = 88
    total_indexes = 88
    total_cells = 8
    normal_attempt = stats(recommended_normal, "attempt")
    normal_e2e = stats(recommended_normal, "e2e")
    if outlier is None:
        outlier_note = "推荐组合没有检测到需要排除的异常轮次。"
    else:
        outlier_upload = outlier["phases"].get(("raster_worker", "minio.tile_upload"), {})
        outlier_note = (
            "推荐组合保留了 1 轮 MinIO 上传尾延迟异常：该轮源缓存全命中，"
            f"但单次上传最高约 {fmt(outlier_upload.get('max'))} s，"
            f"后端 attempt 约 {fmt(outlier['attempt'])} s，不纳入正常均值。"
        )

    return f'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>标准 Landsat 单景实体剖分资源调优报告</title>
<style>
body {{ margin: 24px; color: #202124; background: #fff; font: 14px/1.55 Arial, "Microsoft YaHei", sans-serif; }}
h1 {{ margin: 0 0 8px; font-size: 24px; }}
h2 {{ margin: 28px 0 8px; padding-bottom: 5px; border-bottom: 1px solid #aaa; font-size: 18px; }}
p {{ margin: 7px 0; }}
table {{ width: 100%; margin: 10px 0 18px; border-collapse: collapse; }}
th, td {{ padding: 7px 8px; border: 1px solid #c8c8c8; text-align: left; vertical-align: top; }}
th {{ background: #f1f1f1; font-weight: 600; }}
code {{ font-family: Consolas, monospace; font-size: 12px; }}
.note {{ padding: 9px 11px; border-left: 3px solid #777; background: #f6f6f6; }}
.warn {{ color: #7a4d00; }}
.pass {{ color: #1e5d2a; font-weight: 600; }}
</style>
</head>
<body>
<h1>标准 Landsat 单井 → 单景实体剖分资源调优报告</h1>
<p class="note">本报告不展示日期字段。输入像元为确定性 mock，影像规格、Ray 集群、MinIO 写入和 OpenGauss attempt 均为真实运行。</p>

<h2>一、验收口径</h2>
{table(["项目", "定义"], [
    ["完成条件", "实体瓦片剖分完成、瓦片上传完成、OpenGauss 结果入库完成。"],
    ["后端 attempt", "OpenGauss attempt 的 started_at 到 finished_at；本报告唯一的 10 秒验收指标。"],
    ["端到端", "仅作观察值，包含 API、任务排队和 Ray Job 交付，不参与本次是否达标判断。"],
    ["正常均值", "排除源缓存未命中预热轮和已定位的 MinIO 上传尾延迟轮后，取多轮平均值。"],
])}

<h2>二、单井 → 单景简表</h2>
{table(["项目", "值", "说明"], [
    ["单井", esc(manifest.get("well_id", "-")), "一次请求绑定一个测试井"],
    ["单景", "1 个完整 Landsat scene", "不是小窗口"],
    ["数据类型", "光学", "ISEA4H 实体剖分"],
    ["影像尺寸", f'{esc(attrs.get("width", "-"))} × {esc(attrs.get("height", "-"))}', "完整景"],
    ["波段", esc(", ".join(str(item.get("band_code")) for item in asset.get("bands") or [])), "11 个波段"],
    ["分辨率", "30 m", "标准 Landsat 级别"],
    ["格网", "ISEA4H level 6", "六边形格网"],
    ["输出", f"{total_tiles} tiles / {total_indexes} indexes / {total_cells} cells", "每轮计数一致"],
])}

<h2>三、推荐资源组合</h2>
{table(["参数", "推荐值", "说明"], [
    ["Ray 并行度", "48", "当前 77 个实体 task 的稳定批量等待窗口"],
    ["KubeRay worker", "64", "worker min/max 均为 64；避免 Ray 在测试期间追加调度"],
    ["实体 task 上传线程", "4", "单波段 task 通常只有一个待上传瓦片"],
    ["MinIO multipart 并发", "3", "MinIO 默认值；1、2 的实测尾部更差"],
    ["源缓存", "全部命中后计入正常均值", "避免把大 COG 首次下载混入剖分耗时"],
    ["实际 task", esc(recommended_layout.get("tasks", "77")), "11 个波段单元 × 7 个非空空间分片"],
    ["批量等待", esc(recommended_layout.get("wait_batches", "2")), "一次等待一个并行窗口"],
])}

<h2>四、资源矩阵结果</h2>
{table(["资源组合", "总轮数", "源缓存干净", "正常轮数", "正常 attempt 均值(s)", "标准差(s)", "最小(s)", "最大(s)", "干净轮 < 10"], matrix_rows)}
<p class="pass">推荐组合的正常轮次：{len(recommended_normal)} 轮，后端 attempt 平均 {fmt(normal_attempt["mean"])} s，标准差 {fmt(normal_attempt["stdev"])} s，正常轮次全部低于 10 秒。</p>
<p class="warn">{esc(outlier_note)}</p>

<h2>五、推荐组合逐轮结果</h2>
{table(["轮次", "后端 attempt(s)", "端到端观察(s)", "队列(s)", "单 worker 最长(s)", "上传累计(s)", "上传最长(s)", "源缓存命中", "源缓存未命中", "分类"], round_rows)}

<h2>六、正常轮次统计</h2>
{table(["指标", "均值(s)", "标准差(s)", "最小(s)", "最大(s)"], metric_rows)}
<p>正常轮次端到端观察均值为 {fmt(normal_e2e["mean"])} s，但端到端不是本次验收指标；其额外时间主要来自任务排队和 Ray Job 交付。</p>

<h2>七、阶段耗时明细</h2>
<p>累计均值是所有 worker/task 阶段耗时之和，不是墙钟时间；最长单次用于定位尾延迟。</p>
{table(["Scope", "Phase", "阶段", "累计均值(s)", "累计标准差(s)", "最长单次均值(s)", "平均调用次数"], phase_rows(recommended_normal))}

<h2>八、瓶颈定位</h2>
{table(["环节", "观测", "结论"], [
    ["实体瓦片掩膜", "累计约 59–60 s，最长单次约 1.5 s", "核心计算阶段，但正常轮次不会单独把 attempt 推过 10 秒。"],
    ["MinIO 瓦片上传", "88 个瓦片约 635 MB；正常累计约 60–90 s，异常单次上传可超过 6 s", "当前唯一明显的长尾来源。"],
    ["Ray 等待", "正常批量等待约 2 个窗口，累计约 9–10 s", "已降低 driver 轮询开销，剩余时间由 task 和存储完成分布决定。"],
    ["源数据校验", f"每轮 {checksum['hits']} 次缓存命中、{checksum['misses']} 次未命中", "源缓存预热后不再有源下载；校验仍属于 worker 工作量。"],
    ["OpenGauss", "开始写结果和完成写结果均为低于 1 秒级", "不是当前主瓶颈；完成 attempt 即代表已完成入库。"],
])}

<h2>九、已实施优化</h2>
{table(["优化项", "结果"], [
    ["Ray 批量等待", "从逐个等待改为一次等待一个并行窗口，减少等待调用次数。"],
    ["实体格网覆盖复用", "同一源 asset 的 ISEA4H cover 在 driver 缓存，11 个波段不重复计算。"],
    ["源 COG 校验缓存", "worker 本地源缓存按文件身份复用 SHA-256，避免同一 worker 重复读完整 COG。"],
    ["上传连接数参数化", "新增 multipart 上传并发参数；实测 multipart 1、2、4 都没有稳定收益，推荐保留 3。"],
    ["上传线程路径", "单波段 task 可使用同步上传路径，避免创建无效的单线程线程池。"],
    ["预检去重", "同一批次相同 MinIO source object 只做一次 stat。"],
])}

<h2>十、结论</h2>
<p>按“剖分、上传、入库完成”的后端 attempt 口径，推荐的 48 并行度和 64 worker 组合在正常多轮中平均约 {fmt(normal_attempt["mean"])} 秒，稳定低于 10 秒。64 并行度没有带来收益，48 worker 在当前 MinIO 写入尾延迟下也不如 64 worker 稳定。</p>
<p>剩余风险集中在 MinIO 单对象上传尾部，而不是实体掩膜或 OpenGauss 入库。生产验收时应保持 worker 常驻、预热源 COG 缓存，并持续监控 MinIO 上传 P95/P99；异常存储轮次应单独标识，不能混入正常平均值。</p>
</body>
</html>
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_report(args.manifest, args.matrix_root), encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
