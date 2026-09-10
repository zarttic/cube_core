#!/usr/bin/env python3
"""Generate a plain, date-free HTML report for the Landsat mock benchmark."""

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
    "workflow.run": "工作流总耗时",
    "driver.bootstrap": "Ray Job driver 启动",
    "task.start_attempt": "启动后端 attempt",
    "grid.cover": "实体格网覆盖",
    "ray.init": "连接 Ray",
    "ray.wait": "等待 Ray task",
    "ray.result_get": "获取 Ray 结果",
    "ray.task_submit": "提交 Ray task",
    "driver.result_merge": "合并 task 结果",
    "source_preflight.minio.stat": "源对象预检",
    "source.cache": "源数据缓存命中/准备",
    "source.download": "源数据下载",
    "source.checksum": "源数据校验",
    "source.raster_open": "打开源栅格",
    "grid.geometry": "读取格网几何",
    "grid.window_calculation": "计算栅格窗口",
    "entity.tile_mask": "实体瓦片掩膜",
    "entity.tile_write": "实体瓦片写入",
    "entity.tile_checksum": "实体瓦片校验",
    "minio.tile_stat": "MinIO 瓦片检查",
    "minio.tile_upload": "MinIO 瓦片上传",
}
SCOPE_ORDER = {
    "ray_job_driver": 0,
    "raster_driver": 1,
    "raster_batch_driver": 2,
    "raster_worker": 3,
    "source_preflight": 4,
}
METRICS = (
    ("partition_execution_sec", "后端 attempt"),
    ("end_to_end_sec", "端到端"),
    ("ray_job_sec", "Ray Job"),
    ("queue_wait_sec", "队列等待"),
    ("ray_driver_internal_sec", "Ray driver 内部"),
    ("worker_internal_max_sec", "单 worker 最长"),
    ("worker_internal_sum_sec", "worker 累计"),
)


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


def load_run(path: Path) -> dict[str, Any]:
    summaries = read_csv(path / "summary.csv")
    if len(summaries) != 1:
        raise ValueError(f"expected one case in {path / 'summary.csv'}")
    raw_cases = json.loads((path / "raw_cases.json").read_text(encoding="utf-8"))
    if len(raw_cases) != 1:
        raise ValueError(f"expected one raw case in {path / 'raw_cases.json'}")
    return {
        "path": path,
        "summary": summaries[0],
        "raw": raw_cases[0],
        "phases": read_csv(path / "phase_timings.csv"),
    }


def parse_labeled_paths(values: list[str], kind: str) -> list[tuple[str, Path]]:
    output = []
    for index, value in enumerate(values, start=1):
        if "=" in value:
            label, raw_path = value.split("=", 1)
        else:
            label, raw_path = f"{kind}-{index}", value
        path = Path(raw_path)
        if not path.is_dir():
            raise FileNotFoundError(path)
        output.append((label, path))
    return output


def result_dataset(run: dict[str, Any]) -> dict[str, Any]:
    result = ((run["raw"].get("final_task") or {}).get("result") or {})
    datasets = result.get("datasets") or []
    return datasets[0] if datasets else {}


def run_row(label: str, run: dict[str, Any], category: str) -> dict[str, Any]:
    summary = run["summary"]
    dataset = result_dataset(run)
    counts = dataset.get("counts") or {}
    return {
        "label": label,
        "category": category,
        "attempt": number(summary.get("partition_execution_sec")),
        "e2e": number(summary.get("end_to_end_sec")),
        "ray": number(summary.get("ray_job_sec")),
        "queue": number(summary.get("queue_wait_sec")),
        "ray_driver": number(summary.get("ray_driver_internal_sec")),
        "worker_max": number(summary.get("worker_internal_max_sec")),
        "worker_sum": number(summary.get("worker_internal_sum_sec")),
        "attempt_pass": str(summary.get("partition_under_10_sec", "")).lower() == "true",
        "e2e_pass": str(summary.get("end_to_end_under_10_sec", "")).lower() == "true",
        "attempt_status": summary.get("attempt_status", ""),
        "final_status": summary.get("final_status", ""),
        "ray_status": summary.get("ray_job_status", ""),
        "tiles": integer(counts.get("tiles")),
        "indexes": integer(counts.get("indexes")),
        "grid_cells": integer(counts.get("grid_cells")),
    }


def layout(run: dict[str, Any]) -> dict[str, Any]:
    dataset = result_dataset(run)
    units = ((dataset.get("timings") or {}).get("units") or [])
    ray_tasks = 0
    cell_tasks = 0
    empty_shards = 0
    parallelisms: set[int] = set()
    hosts: set[str] = set()
    for unit in units:
        driver = unit.get("driver") or {}
        counters = driver.get("counters") or {}
        attributes = driver.get("attributes") or {}
        ray_tasks += integer(counters.get("ray_task_submit_count"))
        cell_tasks += integer(counters.get("entity_cell_task_count"))
        empty_shards += integer(counters.get("entity_empty_shard_count"))
        parallelism = integer(attributes.get("ray_parallelism"))
        if parallelism:
            parallelisms.add(parallelism)
        for worker in unit.get("workers") or []:
            hostname = (worker.get("attributes") or {}).get("worker_hostname")
            if hostname:
                hosts.add(str(hostname))
    counts = dataset.get("counts") or {}
    return {
        "units": len(units),
        "grid_cells": integer(counts.get("grid_cells")),
        "ray_tasks": ray_tasks,
        "cell_tasks": cell_tasks,
        "empty_shards": empty_shards,
        "worker_results": sum(len(unit.get("workers") or []) for unit in units),
        "worker_hosts": len(hosts),
        "parallelism": ", ".join(str(item) for item in sorted(parallelisms)) or "-",
        "tiles": integer(counts.get("tiles")),
        "indexes": integer(counts.get("indexes")),
    }


def aggregate_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for key, label in METRICS:
        values = [metric_value(row, key) for row in rows]
        values = [value for value in values if value is not None]
        output.append({
            "label": label,
            "mean": statistics.mean(values) if values else None,
            "stdev": statistics.stdev(values) if len(values) >= 2 else 0.0 if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
        })
    return output


def metric_value(row: dict[str, Any], key: str) -> float | None:
    aliases = {
        "partition_execution_sec": "attempt",
        "end_to_end_sec": "e2e",
        "ray_job_sec": "ray",
        "queue_wait_sec": "queue",
        "ray_driver_internal_sec": "ray_driver",
        "worker_internal_max_sec": "worker_max",
        "worker_internal_sum_sec": "worker_sum",
    }
    return row.get(aliases.get(key, key))


def aggregate_phases(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    per_round: dict[tuple[str, str], dict[int, dict[str, float]]] = defaultdict(dict)
    for round_index, run in enumerate(runs, start=1):
        grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for row in run["phases"]:
            grouped[(row.get("scope", ""), row.get("phase", ""))].append(row)
        for key, phase_rows in grouped.items():
            elapsed = [number(row.get("phase_elapsed_sec")) or 0.0 for row in phase_rows]
            counts = [integer(row.get("phase_count"), 1) for row in phase_rows]
            per_round[key][round_index] = {
                "total": sum(elapsed),
                "max": max(elapsed) if elapsed else 0.0,
                "count": sum(counts),
            }
    output = []
    for (scope, phase), rounds in sorted(
        per_round.items(),
        key=lambda item: (SCOPE_ORDER.get(item[0][0], 99), item[0][1]),
    ):
        totals = [item["total"] for item in rounds.values()]
        maxima = [item["max"] for item in rounds.values()]
        counts = [item["count"] for item in rounds.values()]
        output.append({
            "scope": scope,
            "phase": phase,
            "label": PHASE_LABELS.get(f"{scope}.{phase}", PHASE_LABELS.get(phase, phase)),
            "rounds": len(rounds),
            "total_mean": statistics.mean(totals),
            "total_min": min(totals),
            "total_max": max(totals),
            "max_mean": statistics.mean(maxima),
            "count_mean": statistics.mean(counts),
        })
    return output


def table(headers: list[str], body: list[list[Any]], classes: str = "") -> str:
    head = "".join(f"<th>{esc(item)}</th>" for item in headers)
    rows = []
    for values in body:
        rows.append("<tr>" + "".join(f"<td>{item}</td>" for item in values) + "</tr>")
    class_attr = f' class="{esc(classes)}"' if classes else ""
    return f"<table{class_attr}><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def source_profile(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    optical = next(item for item in manifest["datasets"] if item.get("data_type") == "optical")
    asset = optical["scenes"][0]["assets"][0]
    attrs = asset.get("attributes") or {}
    return {
        "well_id": manifest.get("well_id", "-"),
        "mode": manifest.get("scene_mode", "-"),
        "source_uri": asset.get("source_uri") or asset.get("cog_uri") or "-",
        "width": attrs.get("width", "-"),
        "height": attrs.get("height", "-"),
        "count": attrs.get("count", len(asset.get("bands") or [])),
        "bands": ", ".join(str(item.get("band_code")) for item in asset.get("bands") or []),
        "checksum": asset.get("checksum", "-"),
    }


def render(args: argparse.Namespace) -> str:
    normal_specs = parse_labeled_paths(args.normal, "normal")
    diagnostic_specs = parse_labeled_paths(args.diagnostic, "diagnostic")
    normal_runs = [load_run(path) for _, path in normal_specs]
    diagnostic_runs = [load_run(path) for _, path in diagnostic_specs]
    normal_rows = [run_row(label, run, "正常样本") for (label, _), run in zip(normal_specs, normal_runs)]
    diagnostic_rows = [run_row(label, run, "诊断样本") for (label, _), run in zip(diagnostic_specs, diagnostic_runs)]
    profile = source_profile(Path(args.manifest))
    layout_row = layout(normal_runs[0])
    metric_rows = aggregate_metrics(normal_rows)
    phase_rows = aggregate_phases(normal_runs)
    warmup_download = sum(
        number(row.get("phase_elapsed_sec")) or 0.0
        for row in (diagnostic_runs[0]["phases"] if diagnostic_runs else [])
        if row.get("scope") == "raster_worker" and row.get("phase") == "source.download"
    )

    overview_body = []
    for row in normal_rows + diagnostic_rows:
        overview_body.append([
            esc(row["label"]),
            esc(row["category"]),
            fmt(row["attempt"]),
            fmt(row["e2e"]),
            fmt(row["ray"]),
            fmt(row["queue"]),
            fmt(row["worker_max"]),
            "通过" if row["attempt_pass"] else "未通过",
            "通过" if row["e2e_pass"] else "未通过",
            esc(f'{row["attempt_status"]}/{row["final_status"]}/{row["ray_status"]}'),
        ])

    stats_body = []
    for row in metric_rows:
        stats_body.append([
            esc(row["label"]),
            fmt(row["mean"]),
            fmt(row["stdev"]),
            fmt(row["min"]),
            fmt(row["max"]),
        ])

    phase_body = []
    for row in phase_rows:
        phase_body.append([
            esc(row["scope"]),
            esc(row["phase"]),
            esc(row["label"]),
            fmt(row["total_mean"]),
            fmt(row["max_mean"]),
            fmt(row["total_min"]),
            fmt(row["total_max"]),
            fmt(row["count_mean"], 1),
        ])

    layout_body = [
        ["场景/波段单元", layout_row["units"], "每个波段一个实体批次单元"],
        ["覆盖格元", layout_row["grid_cells"], "所有波段单元共用同一实体覆盖"],
        ["Ray task", layout_row["ray_tasks"], "各波段单元的非空空间分片执行数"],
        ["实体 cell task", layout_row["cell_tasks"], "worker 结果中的 cell 处理数"],
        ["空分片", layout_row["empty_shards"], "30 个空间分片中未产生 cell 的分片数"],
        ["worker 结果", layout_row["worker_results"], "各波段单元返回的 worker 结果数"],
        ["worker 主机数", layout_row["worker_hosts"], "正常样本中观测到的不同 worker"],
        ["Ray 并行度", layout_row["parallelism"], "每个实体批次的配置并行度"],
        ["实体瓦片/索引", f'{layout_row["tiles"]}/{layout_row["indexes"]}', "正常样本输出计数"],
    ]

    normal_attempt = [row["attempt"] for row in normal_rows if row["attempt"] is not None]
    normal_e2e = [row["e2e"] for row in normal_rows if row["e2e"] is not None]
    attempt_pass = sum(row["attempt_pass"] for row in normal_rows)
    e2e_pass = sum(row["e2e_pass"] for row in normal_rows)
    phase_note = (
        "阶段表中的“平均累计”是五轮中同一阶段所有 worker/task 调用耗时的累计均值，"
        "不能直接相加当作墙钟时间；“平均最长单次”用于观察单个调用的最长耗时。"
    )
    return f'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>Landsat 标准单景实体剖分性能报告</title>
<style>
body {{ margin: 24px; color: #202124; background: #fff; font: 14px/1.55 Arial, "Microsoft YaHei", sans-serif; }}
h1 {{ margin: 0 0 8px; font-size: 24px; }}
h2 {{ margin: 28px 0 8px; padding-bottom: 5px; border-bottom: 1px solid #bbb; font-size: 18px; }}
p {{ margin: 7px 0; }}
table {{ width: 100%; margin: 10px 0 18px; border-collapse: collapse; }}
th, td {{ padding: 7px 8px; border: 1px solid #c8c8c8; text-align: left; vertical-align: top; }}
th {{ background: #f1f1f1; font-weight: 600; }}
code {{ font-family: Consolas, monospace; font-size: 12px; }}
.note {{ padding: 9px 11px; border-left: 3px solid #777; background: #f6f6f6; }}
.fail {{ color: #8a1c1c; font-weight: 600; }}
.pass {{ color: #1e5d2a; font-weight: 600; }}
</style>
</head>
<body>
<h1>Landsat 标准单景实体剖分性能报告</h1>
<p class="note">本报告使用 Landsat 8/9 OLI/TIRS 单景尺寸、统一 30 m 网格和 11 波段契约生成的确定性 mock 影像。它用于验证标准负载的处理性能，不代表真实卫星像元值。</p>

<h2>一、单井 → 单景口径</h2>
{table(["项目", "值", "说明"], [
    ["单井", esc(profile["well_id"]), "一次请求只绑定一个测试井"],
    ["单景", "1 个完整 Landsat mock scene", "scene_mode=full，不使用裁剪窗口"],
    ["数据类型", "光学", "本轮只测 optical 实体剖分"],
    ["格网", "isea4h", "六边形格网，level 6"],
    ["剖分方式", "entity", "生成实体瓦片并写入索引"],
    ["Ray 配置", "30", "36 个预热 worker，单 worker 1 CPU/2 GiB"],
])}

<h2>二、输入景规格</h2>
{table(["项目", "值"], [
    ["空间覆盖假设", "185 km × 180 km"],
    ["统一分辨率", "30 m"],
    ["影像尺寸", f'{esc(profile["width"])} × {esc(profile["height"])}'],
    ["波段数", esc(profile["count"])],
    ["波段", esc(profile["bands"])],
    ["源格式", "Cloud Optimized GeoTIFF / uint16"],
    ["CRS", "EPSG:32649"],
    ["源对象", f'<code>{esc(profile["source_uri"])}</code>'],
    ["源 SHA-256", f'<code>{esc(profile["checksum"])}</code>'],
])}

<h2>三、任务拆分与输出</h2>
{table(["指标", "值", "含义"], [[esc(a), esc(b), esc(c)] for a, b, c in layout_body])}

<h2>四、各轮耗时</h2>
<p>正常均值只使用 5 个缓存命中且调度正常的样本；预热、首次缓存命中轮和 Ray 调度异常轮保留在下表，但不进入正常均值。</p>
{table(["样本", "类别", "后端 attempt(s)", "端到端(s)", "Ray Job(s)", "队列(s)", "单 worker 最长(s)", "attempt < 10", "端到端 < 10", "状态"], overview_body)}

<h2>五、正常样本统计</h2>
{table(["指标", "平均(s)", "标准差(s)", "最小(s)", "最大(s)"], stats_body)}
<p>正常样本后端严格小于 10 秒：<span class="{'pass' if attempt_pass == len(normal_rows) else 'fail'}">{attempt_pass}/{len(normal_rows)}</span>；端到端严格小于 10 秒：<span class="{'pass' if e2e_pass == len(normal_rows) else 'fail'}">{e2e_pass}/{len(normal_rows)}</span>。</p>
<p>后端 attempt 平均为 <strong>{fmt(statistics.mean(normal_attempt))} s</strong>，端到端平均为 <strong>{fmt(statistics.mean(normal_e2e))} s</strong>。按“平均值也必须严格小于 10 秒”的口径，后端和端到端均未通过。</p>

<h2>六、阶段耗时</h2>
<p>{esc(phase_note)}</p>
{table(["Scope", "Phase", "阶段含义", "平均累计(s)", "平均最长单次(s)", "累计最小(s)", "累计最大(s)", "平均调用次数"], phase_body)}

<h2>七、缓存与异常说明</h2>
<p>预热轮所有 worker 的源缓存下载阶段累计约 {fmt(warmup_download)} s，这不是单次墙钟下载值，具体下载由多个 worker 并行完成；正常样本的 <code>source.download</code> 为 0，说明 worker 本地源缓存已命中。</p>
<p>正常样本仍需要对缓存文件做 SHA-256、打开栅格、读取格网几何、执行实体掩膜、写瓦片、检查并上传 MinIO 对象，因此缓存命中不会消除主体计算。正常样本的阶段表中，源校验、实体掩膜、瓦片写入和上传仍然可见。</p>
<p>诊断样本中的 Ray 调度异常轮虽然 attempt/final/Ray Job 都成功，但 Ray 等待阶段显著拉长，属于集群调度波动，已从正常均值剔除；它没有被当作业务失败。</p>

<h2>八、结论</h2>
<p class="fail">在标准 Landsat 单景 mock 口径、30 m、11 波段、ISEA4H level 6、Ray 并行度 30 的条件下，正常缓存命中轮次的后端平均约 {fmt(statistics.mean(normal_attempt))} 秒，端到端平均约 {fmt(statistics.mean(normal_e2e))} 秒。后端仅在部分轮次低于 10 秒，端到端没有轮次低于 10 秒，因此不能判定为稳定满足 10 秒要求。</p>
<p>该结果与上一轮 10 m、10 波段 Sentinel mock 不可直接横向比较；本报告采用的是统一 30 m 的 Landsat 标准单景负载。</p>
</body>
</html>
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--normal", action="append", required=True, help="label=run directory")
    parser.add_argument("--diagnostic", action="append", default=[], help="label=run directory")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(args), encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
