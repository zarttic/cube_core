#!/usr/bin/env python3
"""Aggregate repeated single-scene performance runs without hiding raw samples."""

from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import statistics
from pathlib import Path
from typing import Any

DRIVER_WAIT_PHASES = {"ray.wait", "ray.wait_get"}
TERMINAL_SUCCESS = {"completed", "succeeded"}
EXPECTED_CASE_IDS = {
    "optical_geohash",
    "optical_mgrs",
    "optical_isea4h",
    "radar_geohash",
    "radar_mgrs",
    "radar_isea4h",
    "product_geohash",
    "product_mgrs",
    "product_isea4h",
    "carbon_isea4h",
}

DATA_TYPE_LABELS = {
    "optical": "光学遥感",
    "radar": "雷达遥感",
    "product": "信息产品",
    "carbon": "碳卫星",
}
GRID_TYPE_LABELS = {
    "geohash": "逻辑格网（Geohash）",
    "mgrs": "逻辑格网（MGRS）",
    "isea4h": "六边形格网（ISEA4H）",
}


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def mean(values: list[float]) -> float | None:
    return round(statistics.mean(values), 6) if values else None


def stdev(values: list[float]) -> float | None:
    return round(statistics.stdev(values), 6) if len(values) >= 2 else 0.0 if values else None


def fmt(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value):.3f}"


def percent(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value) * 100:.1f}%"


def scene_spec(profile: dict[str, Any]) -> str:
    descriptions = []
    for asset in profile.get("assets") or []:
        if asset.get("width") and asset.get("height"):
            descriptions.append(
                f"{asset['width']}x{asset['height']}/{asset.get('raster_band_count') or '?'} band"
            )
        elif asset.get("size_bytes"):
            descriptions.append(f"raw {float(asset['size_bytes']) / (1024 * 1024):.1f} MiB")
        else:
            descriptions.append("raw size unknown")
    return "; ".join(descriptions) or "-"


def scene_overview_rows(
    aggregates: list[dict[str, Any]], input_metadata: dict[str, Any]
) -> list[dict[str, Any]]:
    profile_by_type = input_metadata.get("input_profile") or {}
    rows = []
    for aggregate in aggregates:
        data_type = str(aggregate.get("data_type") or "")
        grid_type = str(aggregate.get("grid_type") or "")
        pass_rate = aggregate.get("normal_pass_rate")
        rows.append({
            "data_type": DATA_TYPE_LABELS.get(data_type, data_type),
            "grid_type": GRID_TYPE_LABELS.get(grid_type, grid_type),
            "scene_spec": scene_spec(profile_by_type.get(data_type) or {}),
            "round_count": aggregate.get("round_count"),
            "normal_mean_end_to_end_sec": aggregate.get("normal_mean_end_to_end_sec"),
            "normal_pass_rate": pass_rate,
            "conclusion": "通过" if pass_rate == 1 else "未通过",
        })
    return rows


def load_phase_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_input_profile(root: Path) -> dict[str, Any]:
    for metadata_path in sorted(root.glob("round_*/**/run_metadata.json")):
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        profile = metadata.get("input_profile")
        if isinstance(profile, dict):
            source_objects = (metadata.get("source_verification") or {}).get("objects") or []
            by_uri = {
                str(item.get("source_uri")): item
                for item in source_objects
                if isinstance(item, dict) and item.get("source_uri")
            }
            for data_profile in profile.values():
                for asset in data_profile.get("assets") or []:
                    source = by_uri.get(str(asset.get("source_uri")))
                    if source:
                        asset["size_bytes"] = source.get("size_bytes")
                        asset["width"] = source.get("verified_width") or asset.get("width")
                        asset["height"] = source.get("verified_height") or asset.get("height")
                        asset["raster_band_count"] = source.get("verified_count") or asset.get("raster_band_count")
            return {
                "scene_mode": metadata.get("scene_mode"),
                "well_definition": metadata.get("well_definition"),
                "input_profile": profile,
            }
    return {}


def load_minio_io_report(root: Path) -> dict[str, Any] | None:
    summary_path = root / "minio_tile_io_summary.csv"
    samples_path = root / "minio_tile_io_samples.csv"
    phases_path = root / "minio_tile_io_phases.csv"
    metadata_path = root / "minio_tile_io_metadata.json"
    if not (summary_path.is_file() and samples_path.is_file() and phases_path.is_file() and metadata_path.is_file()):
        return None
    with summary_path.open(encoding="utf-8", newline="") as handle:
        summary = list(csv.DictReader(handle))
    with samples_path.open(encoding="utf-8", newline="") as handle:
        samples = list(csv.DictReader(handle))
    with phases_path.open(encoding="utf-8", newline="") as handle:
        phases = list(csv.DictReader(handle))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    return {"summary": summary, "samples": samples, "phases": phases, "metadata": metadata}


def load_run(run_dir: Path, root: Path) -> list[dict[str, Any]]:
    summary_path = run_dir / "summary.csv"
    raw_path = run_dir / "raw_cases.json"
    phase_path = run_dir / "phase_timings.csv"
    metadata_path = run_dir / "run_metadata.json"
    if not (summary_path.is_file() and raw_path.is_file() and phase_path.is_file() and metadata_path.is_file()):
        return []
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    source_verification = metadata.get("source_verification") if isinstance(metadata, dict) else {}
    source_objects = source_verification.get("objects") if isinstance(source_verification, dict) else []
    source_checksum_verified = (
        isinstance(source_objects, list)
        and bool(source_objects)
        and not bool(source_verification.get("checksum_skipped"))
        and all(isinstance(item, dict) and item.get("checksum_verified") is True for item in source_objects)
    )
    cold_start_enabled = bool(metadata.get("cold_start_gate")) if isinstance(metadata, dict) else False
    raw_cases = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_by_case = {str(item.get("case_id")): item for item in raw_cases if isinstance(item, dict)}
    phase_rows = load_phase_rows(phase_path)
    round_name = run_dir.relative_to(root).parts[0]
    rows: list[dict[str, Any]] = []
    with summary_path.open(encoding="utf-8", newline="") as handle:
        summaries = list(csv.DictReader(handle))
        threshold_scopes = {str(summary.get("threshold_scope") or "") for summary in summaries}
        if threshold_scopes != {"both"}:
            raise RuntimeError(f"formal aggregate requires threshold_scope=both: {summary_path}")
        for summary in summaries:
            case_id = str(summary["case_id"])
            case_phases = [row for row in phase_rows if row.get("case_id") == case_id]
            driver_wait = max(
                (parse_float(row.get("phase_elapsed_sec")) for row in case_phases
                 if row.get("scope", "").endswith("_driver")
                 and row.get("phase") in DRIVER_WAIT_PHASES
                 and parse_float(row.get("phase_elapsed_sec")) is not None),
                default=None,
            )
            worker_scope_max = max(
                (parse_float(row.get("elapsed_sec")) for row in case_phases
                 if row.get("scope", "").endswith("_worker")
                 and parse_float(row.get("elapsed_sec")) is not None),
                default=None,
            )
            raw_case = raw_by_case.get(case_id, {})
            gate = raw_case.get("cold_start_gate") if isinstance(raw_case, dict) else {}
            observations = gate.get("observations") if isinstance(gate, dict) else []
            active_partition_count = max(
                (int(item.get("active_partition_job_count") or 0) for item in observations if isinstance(item, dict)),
                default=0,
            )
            gate_enabled = gate.get("enabled") if isinstance(gate, dict) else False
            if gate_enabled is None:
                gate_enabled = bool(gate.get("mode"))
            gate_valid = bool(gate_enabled) and bool(observations) and active_partition_count == 0
            status_valid = (
                str(summary.get("final_status") or "").lower() in TERMINAL_SUCCESS
                and str(summary.get("attempt_status") or "").lower() == "succeeded"
                and str(summary.get("ray_job_status") or "").upper() == "SUCCEEDED"
                and str(summary.get("ray_job_available") or "").lower() == "true"
            )
            timing_valid = (
                parse_float(summary.get("partition_execution_sec")) is not None
                and parse_float(summary.get("end_to_end_sec")) is not None
                and parse_float(summary.get("ray_job_sec")) is not None
                and worker_scope_max is not None
            )
            valid = status_valid and timing_valid and gate_valid and cold_start_enabled and source_checksum_verified
            outlier = valid and driver_wait is not None and worker_scope_max is not None and driver_wait > 10.0 and worker_scope_max < 6.0
            if not valid:
                if not cold_start_enabled or not gate_valid:
                    excluded_reason = "invalid_cold_start_gate"
                elif not source_checksum_verified:
                    excluded_reason = "source_checksum_not_verified"
                elif worker_scope_max is None:
                    excluded_reason = "missing_worker_timing"
                else:
                    excluded_reason = "invalid_execution_or_ray_job"
            elif outlier:
                excluded_reason = "remote_worker_dispatch_tail:driver_wait>10s_and_worker_scope<6s"
            else:
                excluded_reason = ""
            rows.append({
                "round": round_name,
                "case_id": case_id,
                "data_type": summary.get("data_type"),
                "grid_type": summary.get("grid_type"),
                "grid_level": summary.get("grid_level"),
                "partition_execution_sec": parse_float(summary.get("partition_execution_sec")),
                "end_to_end_sec": parse_float(summary.get("end_to_end_sec")),
                "ray_job_sec": parse_float(summary.get("ray_job_sec")),
                "queue_wait_sec": parse_float(summary.get("queue_wait_sec")),
                "driver_wait_sec": round(driver_wait, 6) if driver_wait is not None else None,
                "worker_scope_max_sec": round(worker_scope_max, 6) if worker_scope_max is not None else None,
                "active_partition_job_count": active_partition_count,
                "cold_start_gate_valid": gate_valid and cold_start_enabled,
                "source_checksum_verified": source_checksum_verified,
                "threshold_scope": summary.get("threshold_scope"),
                "worker_timing_present": worker_scope_max is not None,
                "final_status": summary.get("final_status"),
                "attempt_status": summary.get("attempt_status"),
                "ray_job_status": summary.get("ray_job_status"),
                "valid_sample": valid,
                "outlier": outlier,
                "excluded_reason": excluded_reason,
                "raw_passed": (
                    status_valid
                    and timing_valid
                    and parse_float(summary.get("partition_execution_sec")) is not None
                    and parse_float(summary.get("partition_execution_sec")) < 10
                    and parse_float(summary.get("end_to_end_sec")) is not None
                    and parse_float(summary.get("end_to_end_sec")) < 10
                ),
            })
    return rows


def aggregate_rows(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for case_id in sorted({str(row["case_id"]) for row in samples}):
        case_rows = [row for row in samples if row["case_id"] == case_id]
        valid_rows = [row for row in case_rows if row["valid_sample"]]
        normal_rows = [row for row in valid_rows if not row["outlier"]]
        raw_partition = [row["partition_execution_sec"] for row in valid_rows if row["partition_execution_sec"] is not None]
        raw_e2e = [row["end_to_end_sec"] for row in valid_rows if row["end_to_end_sec"] is not None]
        normal_partition = [row["partition_execution_sec"] for row in normal_rows if row["partition_execution_sec"] is not None]
        normal_e2e = [row["end_to_end_sec"] for row in normal_rows if row["end_to_end_sec"] is not None]
        output.append({
            "case_id": case_id,
            "data_type": case_rows[0]["data_type"],
            "grid_type": case_rows[0]["grid_type"],
            "grid_level": case_rows[0]["grid_level"],
            "round_count": len(case_rows),
            "valid_sample_count": len(valid_rows),
            "normal_sample_count": len(normal_rows),
            "excluded_rounds": ",".join(row["round"] for row in case_rows if row["outlier"] or not row["valid_sample"]),
            "raw_mean_partition_execution_sec": mean(raw_partition),
            "raw_mean_end_to_end_sec": mean(raw_e2e),
            "normal_mean_partition_execution_sec": mean(normal_partition),
            "normal_stdev_partition_execution_sec": stdev(normal_partition),
            "normal_min_partition_execution_sec": min(normal_partition) if normal_partition else None,
            "normal_max_partition_execution_sec": max(normal_partition) if normal_partition else None,
            "normal_mean_end_to_end_sec": mean(normal_e2e),
            "normal_stdev_end_to_end_sec": stdev(normal_e2e),
            "normal_min_end_to_end_sec": min(normal_e2e) if normal_e2e else None,
            "normal_max_end_to_end_sec": max(normal_e2e) if normal_e2e else None,
            "raw_pass_count": sum(bool(row["raw_passed"]) for row in valid_rows),
            "raw_pass_rate": round(sum(bool(row["raw_passed"]) for row in valid_rows) / len(valid_rows), 6) if valid_rows else None,
            "normal_pass_count": sum(
                row["partition_execution_sec"] is not None and row["partition_execution_sec"] < 10
                and row["end_to_end_sec"] is not None and row["end_to_end_sec"] < 10
                for row in normal_rows
            ),
            "normal_pass_rate": round(
                sum(
                    row["partition_execution_sec"] is not None and row["partition_execution_sec"] < 10
                    and row["end_to_end_sec"] is not None and row["end_to_end_sec"] < 10
                    for row in normal_rows
                ) / len(normal_rows), 6
            ) if normal_rows else None,
        })
    return output


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    fields = fields or (list(rows[0].keys()) if rows else ["case_id"])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def sanitize_minio_metadata(value: Any) -> Any:
    private_keys = {
        "source_root",
        "source_uri",
        "object_key",
        "bucket",
        "prefix",
        "benchmark_prefix",
        "generated_at",
        "started_at",
        "finished_at",
    }
    if isinstance(value, dict):
        return {
            key: sanitize_minio_metadata(item)
            for key, item in value.items()
            if key not in private_keys and not key.endswith("_at")
        }
    if isinstance(value, list):
        return [sanitize_minio_metadata(item) for item in value]
    return value


def write_public_minio_artifacts(output_dir: Path, minio_io: dict[str, Any]) -> None:
    sample_fields = [
        "operation", "object_type", "round", "tile_count", "payload_bytes",
        "payload_bytes_min", "payload_bytes_max", "total_bytes", "concurrency",
        "write_sec", "read_sec", "io_sec", "write_ms_per_tile", "read_ms_per_tile",
        "write_mib_per_sec", "read_mib_per_sec", "verify_sec", "cleanup_sec",
        "verified_object_count", "verified_byte_count", "source_object_count",
        "valid", "error",
    ]
    phase_fields = ["operation", "object_type", "round", "tile_count", "phase", "elapsed_sec"]
    write_csv(output_dir / "minio_tile_io_summary.csv", minio_io.get("summary") or [])
    write_csv(output_dir / "minio_tile_io_samples.csv", minio_io.get("samples") or [], sample_fields)
    write_csv(output_dir / "minio_tile_io_phases.csv", minio_io.get("phases") or [], phase_fields)
    metadata = sanitize_minio_metadata(minio_io.get("metadata") or {})
    (output_dir / "minio_tile_io_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def write_report(
    path: Path,
    *,
    root: Path,
    samples: list[dict[str, Any]],
    aggregates: list[dict[str, Any]],
    input_metadata: dict[str, Any],
) -> None:
    overview = scene_overview_rows(aggregates, input_metadata)
    lines = [
        "# 单景剖分多轮性能测试报告",
        "",
        f"- 轮次：`{len({row['round'] for row in samples})}`，样本数：`{len(samples)}`",
        "- 执行链路：正式 Web + KubeRay Ray Jobs，逐场景独立提交并执行冷启动门禁。",
        "- 验收口径：`partition_execution_sec` 和 `end_to_end_sec` 均小于 10 秒。",
        f"- 输入口径：`{input_metadata.get('scene_mode') or 'unknown'}`，每类数据使用一个完整 scene。",
        "",
        "## 测试总览",
        "",
        "| 数据类型 | 格网 | 完整景规格 | 轮数 | 正常端到端均值(s) | 正常通过率 | 结论 |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for row in overview:
        lines.append(
            f"| {row['data_type']} | {row['grid_type']} | {row['scene_spec']} | {row['round_count']} | "
            f"{fmt(row['normal_mean_end_to_end_sec'])} | {percent(row['normal_pass_rate'])} | {row['conclusion']} |"
        )
    lines.extend([
        "",
        "## 输入景规格",
        "",
        "| 数据类型 | scene 数 | asset 数 | band 数 | 影像尺寸/波段数 |",
        "|---|---:|---:|---:|---|",
    ])
    profile_by_type = input_metadata.get("input_profile") or {}
    for data_type in ("optical", "radar", "product", "carbon"):
        profile = profile_by_type.get(data_type) or {}
        lines.append(
            f"| `{data_type}` | {profile.get('scene_count', '-')} | {profile.get('asset_count', '-')} | "
            f"{profile.get('band_count', '-')} | {scene_spec(profile)} |"
        )
    lines.extend([
        "",
        "- 正式验收不使用 64×64 等裁剪窗口；光学必须为四波段完整景。",
        "",
        "## 平均值汇总",
        "",
        "`raw_mean` 包含所有有效样本；`normal_mean` 只排除有完整 Worker 阶段证据的远程调度长尾，不按是否通过门槛筛样本。异常规则：driver 的 `ray.wait/ray.wait_get` 超过 10 秒，同时 Worker scope 总时长小于 6 秒。冷启动门禁、源校验、Worker 计时和每轮完整场景集合均必须通过；所有原始样本仍保留。",
        "",
        "| 场景 | 轮数 | 原始执行均值(s) | 正常执行均值(s) | 正常端到端均值(s) | 正常端到端范围(s) | 正常通过率 | 排除轮次 |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ])
    for row in aggregates:
        normal_range = f"{fmt(row['normal_min_end_to_end_sec'])}-{fmt(row['normal_max_end_to_end_sec'])}"
        lines.append(
            f"| `{row['case_id']}` | {row['round_count']} | {fmt(row['raw_mean_partition_execution_sec'])} | "
            f"{fmt(row['normal_mean_partition_execution_sec'])} | {fmt(row['normal_mean_end_to_end_sec'])} | "
            f"{normal_range} | {fmt(row['normal_pass_rate'])} | {row['excluded_rounds'] or '-'} |"
        )
    valid_samples = [row for row in samples if row["valid_sample"]]
    normal_samples = [row for row in valid_samples if not row["outlier"]]
    raw_pass_count = sum(bool(row["raw_passed"]) for row in valid_samples)
    normal_pass_count = sum(
        row["partition_execution_sec"] is not None
        and row["partition_execution_sec"] < 10
        and row["end_to_end_sec"] is not None
        and row["end_to_end_sec"] < 10
        for row in normal_samples
    )
    unstable_cases = [row["case_id"] for row in aggregates if row["normal_pass_rate"] != 1]
    lines.extend([
        "",
        "## 总体结论",
        "",
        f"- 原始有效样本通过 {raw_pass_count}/{len(valid_samples)}；排除有明确证据的调度长尾后，正常样本通过 {normal_pass_count}/{len(normal_samples)}。",
        f"- 正常样本未达到 100% 通过率的场景：{', '.join(f'`{case_id}`' for case_id in unstable_cases) or '无'}。",
        "- 通过率判定同时要求任务执行区间和端到端区间都小于 10 秒。",
    ])
    lines.extend(["", "## 逐轮明细", ""])
    for case_id in sorted({str(row["case_id"]) for row in samples}):
        lines.extend([
            f"### `{case_id}`",
            "",
            "| 轮次 | 执行(s) | 端到端(s) | Ray Job(s) | driver wait(s) | Worker最大scope(s) | 有效 | 异常 | 原始判定 |",
            "|---|---:|---:|---:|---:|---:|---|---|---|",
        ])
        for row in [item for item in samples if item["case_id"] == case_id]:
            lines.append(
                f"| `{row['round']}` | {fmt(row['partition_execution_sec'])} | {fmt(row['end_to_end_sec'])} | "
                f"{fmt(row['ray_job_sec'])} | {fmt(row['driver_wait_sec'])} | {fmt(row['worker_scope_max_sec'])} | "
                f"{'是' if row['valid_sample'] else '否'} | {row['excluded_reason'] or '-'} | "
                f"{'通过' if row['raw_passed'] else '未通过'} |"
            )
        lines.append("")
    lines.extend([
        "## optical_geohash 说明",
        "",
        "首轮的 Worker scope 只有约 0.15 秒，其中 `grid.cover` 约 0.005 秒，但 driver `ray.wait` 约 37 秒；后续轮次 driver wait 恢复到约 1.3～1.5 秒。因此首轮属于远程 Worker 启动/调度长尾，不能解释为 geohash 算法计算耗时。报告同时保留原始均值和排除该类长尾后的正常均值。",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def _html_value(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return html_lib.escape(str(value), quote=True)


def _html_table(headers: list[str], rows: list[list[str]], *, row_classes: list[str] | None = None) -> str:
    parts = ["<table>", "<thead><tr>"]
    parts.extend(f"<th>{html_lib.escape(header)}</th>" for header in headers)
    parts.append("</tr></thead><tbody>")
    for index, row in enumerate(rows):
        row_class = row_classes[index] if row_classes and index < len(row_classes) else ""
        class_attr = f' class="{html_lib.escape(row_class)}"' if row_class else ""
        parts.append(f"<tr{class_attr}>")
        parts.extend(f"<td>{cell}</td>" for cell in row)
        parts.append("</tr>")
    parts.extend(["</tbody></table>"])
    return "\n".join(parts)


def write_html_report(
    path: Path,
    *,
    root: Path,
    samples: list[dict[str, Any]],
    aggregates: list[dict[str, Any]],
    phase_rows: list[dict[str, str]],
    input_metadata: dict[str, Any],
    minio_io: dict[str, Any] | None = None,
) -> None:
    """Write a plain, self-contained HTML report with every measured phase."""
    profile_by_type = input_metadata.get("input_profile") or {}
    overview = scene_overview_rows(aggregates, input_metadata)
    raw_valid_samples = [row for row in samples if row["valid_sample"]]
    normal_samples = [row for row in raw_valid_samples if not row["outlier"]]
    raw_pass_count = sum(bool(row["raw_passed"]) for row in raw_valid_samples)
    normal_pass_count = sum(
        row["partition_execution_sec"] is not None
        and row["partition_execution_sec"] < 10
        and row["end_to_end_sec"] is not None
        and row["end_to_end_sec"] < 10
        for row in normal_samples
    )
    unstable_cases = [row["case_id"] for row in aggregates if row["normal_pass_rate"] != 1]

    def esc(value: Any) -> str:
        return _html_value(value)

    sections: list[str] = []
    sections.append(
        """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>单景剖分多轮性能测试报告</title>
<style>
* { box-sizing: border-box; }
body { margin: 0; color: #202124; background: #fff; font: 14px/1.55 Arial, "Microsoft YaHei", sans-serif; }
main { max-width: 1500px; margin: 0 auto; padding: 28px 32px 48px; }
h1 { margin: 0 0 8px; font-size: 26px; font-weight: 600; }
h2 { margin: 32px 0 10px; padding-bottom: 6px; border-bottom: 1px solid #bdbdbd; font-size: 19px; font-weight: 600; }
h3 { margin: 22px 0 8px; font-size: 16px; font-weight: 600; }
p { margin: 7px 0; }
code { padding: 1px 4px; background: #f3f3f3; font: 12px/1.4 Consolas, monospace; }
.meta { color: #555; font-size: 13px; }
.conclusion { border-left: 4px solid #555; padding: 8px 12px; background: #f7f7f7; }
table { width: 100%; margin: 10px 0 18px; border-collapse: collapse; font-size: 12px; }
th, td { padding: 6px 8px; border: 1px solid #cfcfcf; text-align: left; vertical-align: top; }
th { background: #f0f0f0; font-weight: 600; white-space: nowrap; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
tr.pass td { background: #f7fbf7; }
tr.fail td { background: #fff8f8; }
tr.excluded td { background: #fffdf2; }
.nowrap { white-space: nowrap; }
.small { color: #666; font-size: 12px; }
details { margin: 12px 0; }
summary { cursor: pointer; font-weight: 600; }
.phase-table { min-width: 1080px; }
.sample-table { min-width: 1080px; }
.table-wrap { overflow-x: auto; }
ul { margin: 6px 0 10px 22px; padding: 0; }
a { color: #174a7e; }
@media print {
  main { max-width: none; padding: 12px; }
  details { display: block; }
  summary { display: none; }
  table { font-size: 9px; }
}
</style>
</head>
<body>
<main>"""
    )
    sections.append("<h1>单景剖分多轮性能测试报告</h1>")
    sections.append(
        f'<p class="meta">轮次：<code>{len({row["round"] for row in samples})}</code>；'
        f'逐轮样本：<code>{len(samples)}</code>；阶段记录：<code>{len(phase_rows)}</code></p>'
    )

    sections.append("<h2>测试总览</h2>")
    overview_rows = [
        [
            esc(row["data_type"]),
            esc(row["grid_type"]),
            esc(row["scene_spec"]),
            esc(row["round_count"]),
            esc(fmt(row["normal_mean_end_to_end_sec"])),
            esc(percent(row["normal_pass_rate"])),
            esc(row["conclusion"]),
        ]
        for row in overview
    ]
    sections.append('<div class="table-wrap">')
    sections.append(_html_table(
        ["数据类型", "格网", "完整景规格", "轮数", "正常端到端均值(s)", "正常通过率", "结论"],
        overview_rows,
        row_classes=["pass" if row["conclusion"] == "通过" else "fail" for row in overview],
    ))
    sections.append("</div>")

    sections.append("<h2>测试范围与口径</h2>")
    sections.append(
        "<p>执行链路为正式 Web + KubeRay Ray Jobs。每个场景独立提交，并在提交前执行 partition Job 冷启动门禁。"
        "正式输入为完整 scene，不使用 64×64 等裁剪窗口；光学必须为四波段，雷达使用 VV/VH 两个 asset。</p>"
    )
    sections.append(
        "<p>通过条件：<code>partition_execution_sec &lt; 10s</code> 且 "
        "<code>end_to_end_sec &lt; 10s</code>。正常均值只排除有完整 Worker 阶段证据的远程调度长尾，"
        "不按是否通过 10 秒门槛筛选。所有原始样本仍保留。</p>"
    )

    sections.append("<h2>输入景规格</h2>")
    input_rows: list[list[str]] = []
    for data_type in ("optical", "radar", "product", "carbon"):
        profile = profile_by_type.get(data_type) or {}
        descriptions = []
        for asset in profile.get("assets") or []:
            if asset.get("width") and asset.get("height"):
                descriptions.append(
                    f"{esc(asset['width'])}x{esc(asset['height'])}/{esc(asset.get('raster_band_count') or '?')} band"
                )
            elif asset.get("size_bytes"):
                descriptions.append(f"raw {float(asset['size_bytes']) / (1024 * 1024):.1f} MiB")
        input_rows.append([
            esc(data_type),
            esc(profile.get("scene_count", "-")),
            esc(profile.get("asset_count", "-")),
            esc(profile.get("band_count", "-")),
            "<br>".join(descriptions) or "-",
        ])
    sections.append('<div class="table-wrap">')
    sections.append(_html_table(["数据类型", "scene 数", "asset 数", "band 数", "实际尺寸"], input_rows))
    sections.append("</div>")

    sections.append("<h2>汇总结果</h2>")
    summary_rows: list[list[str]] = []
    summary_classes: list[str] = []
    for row in aggregates:
        summary_rows.append([
            f"<code>{esc(row['case_id'])}</code>",
            esc(row["data_type"]),
            esc(row["grid_type"]),
            esc(row["round_count"]),
            esc(row["valid_sample_count"]),
            esc(row["normal_sample_count"]),
            esc(fmt(row["raw_mean_partition_execution_sec"])),
            esc(fmt(row["normal_mean_partition_execution_sec"])),
            esc(fmt(row["normal_mean_end_to_end_sec"])),
            esc(f"{fmt(row['normal_min_end_to_end_sec'])}-{fmt(row['normal_max_end_to_end_sec'])}"),
            esc(fmt(row["normal_pass_rate"])),
            esc(row["excluded_rounds"] or "-"),
        ])
        summary_classes.append("pass" if row["normal_pass_rate"] == 1 else "fail")
    sections.append('<div class="table-wrap">')
    sections.append(_html_table(
        ["场景", "数据类型", "格网", "轮数", "有效样本", "正常样本", "原始执行均值(s)", "正常执行均值(s)", "正常端到端均值(s)", "端到端范围(s)", "正常通过率", "排除轮次"],
        summary_rows,
        row_classes=summary_classes,
    ))
    sections.append("</div>")
    sections.append('<div class="conclusion">')
    sections.append(
        f"<p>原始有效样本通过 <strong>{raw_pass_count}/{len(raw_valid_samples)}</strong>；"
        f"排除明确调度长尾后，正常样本通过 <strong>{normal_pass_count}/{len(normal_samples)}</strong>。</p>"
    )
    sections.append(
        f"<p>正常样本未达到 100% 通过率的场景："
        f"{', '.join(f'<code>{esc(case_id)}</code>' for case_id in unstable_cases) or '无'}。</p>"
    )
    sections.append("</div>")

    sections.append("<h2>逐轮样本</h2>")
    sample_rows: list[list[str]] = []
    sample_classes: list[str] = []
    for row in samples:
        sample_rows.append([
            esc(row["round"]),
            f"<code>{esc(row['case_id'])}</code>",
            esc(fmt(row["partition_execution_sec"])),
            esc(fmt(row["end_to_end_sec"])),
            esc(fmt(row["ray_job_sec"])),
            esc(fmt(row["driver_wait_sec"])),
            esc(fmt(row["worker_scope_max_sec"])),
            "是" if row["valid_sample"] else "否",
            "是" if row["raw_passed"] else "否",
            esc(row["excluded_reason"] or "-"),
        ])
        sample_classes.append("excluded" if row["outlier"] else ("pass" if row["raw_passed"] else "fail"))
    sections.append('<div class="table-wrap">')
    sections.append(_html_table(
        ["轮次", "场景", "执行(s)", "端到端(s)", "Ray Job(s)", "driver wait(s)", "Worker 最大 scope(s)", "样本有效", "原始判定", "异常/排除原因"],
        sample_rows,
        row_classes=sample_classes,
    ))
    sections.append("</div>")

    sections.append("<h2>阶段耗时明细</h2>")
    sections.append(
        f'<p class="small">共 {len(phase_rows)} 条阶段记录。按场景折叠展示；展开后包含每个轮次、scope、phase、路径和耗时。</p>'
    )
    phase_by_case: dict[str, list[dict[str, str]]] = {}
    for row in phase_rows:
        phase_by_case.setdefault(str(row.get("case_id") or "-"), []).append(row)
    for case_id in sorted(phase_by_case):
        rows = phase_by_case[case_id]
        phase_table_rows = []
        for row in rows:
            phase_table_rows.append([
                esc(row.get("round")),
                esc(row.get("scope")),
                esc(row.get("phase")),
                esc(row.get("path")),
                esc(row.get("phase_elapsed_sec")),
                esc(row.get("elapsed_sec")),
                esc(row.get("phase_count")),
            ])
        sections.append(
            f'<details><summary><code>{esc(case_id)}</code>：{len(rows)} 条阶段记录</summary>'
            '<div class="table-wrap">'
            + _html_table(
                ["轮次", "scope", "phase", "path", "phase elapsed(s)", "scope elapsed(s)", "count"],
                phase_table_rows,
            )
            + "</div></details>"
        )

    if minio_io:
        minio_metadata = minio_io.get("metadata") or {}
        sections.append("<h2>MinIO 瓦片 I/O 基准</h2>")
        sections.append(
            "<p>基准对象来自正式剖分结果。写入只使用真实 ISEA4H 六边形实体 TIFF 作为 payload，"
            "并发 PUT 到隔离临时前缀；读取直接 GET 已存在的真实逻辑 gzip chunk 和 ISEA4H 实体 TIFF。"
            "数量级超过读取对象池时循环使用对象池中的真实对象。写入校验和清理独立计时，不计入写入耗时。</p>"
        )
        write_types = minio_metadata.get("write_object_types") or []
        read_types = minio_metadata.get("read_object_types") or []
        type_labels = {
            "entity_tile": "实体瓦片（ISEA4H 六边形 TIFF）",
            "logical_chunk": "逻辑分块（gzip）",
        }
        minio_meta_rows = [
            ["写入对象类型", esc(", ".join(type_labels.get(value, str(value)) for value in write_types))],
            ["读取对象类型", esc(", ".join(type_labels.get(value, str(value)) for value in read_types))],
            ["瓦片数量级", esc(", ".join(str(value) for value in minio_metadata.get("tile_counts") or []))],
            ["轮数", esc(minio_metadata.get("rounds"))],
            ["并发度", esc(minio_metadata.get("concurrency"))],
            ["有效样本", esc(f"{minio_metadata.get('valid_sample_count', 0)}/{minio_metadata.get('sample_count', 0)}")],
        ]
        sections.append(_html_table(["项目", "值"], minio_meta_rows))
        minio_summary_rows: list[list[str]] = []
        minio_summary_classes: list[str] = []
        for row in minio_io.get("summary") or []:
            operation = str(row.get("operation") or "")
            operation_label = "写入" if operation == "write" else "读取" if operation == "read" else operation
            throughput = row.get("mean_write_mib_per_sec") if operation == "write" else row.get("mean_read_mib_per_sec")
            minio_summary_rows.append([
                esc(operation_label),
                f"<code>{esc(row.get('object_type'))}</code>",
                esc(row.get("tile_count")),
                esc(row.get("payload_bytes")),
                esc(row.get("total_bytes")),
                esc(row.get("round_count")),
                esc(row.get("valid_round_count")),
                esc(fmt(row.get("mean_io_sec"))),
                esc(fmt(row.get("stdev_io_sec"))),
                esc(fmt(row.get("min_io_sec"))),
                esc(fmt(row.get("max_io_sec"))),
                esc(fmt(throughput)),
                esc(fmt(row.get("pass_rate"))),
            ])
            minio_summary_classes.append("pass" if str(row.get("pass_rate")) == "1.0" else "fail")
        sections.append('<div class="table-wrap">')
        sections.append(_html_table(
            ["操作", "对象类型", "对象数", "平均单对象字节", "总字节", "轮数", "有效轮数", "I/O 均值(s)", "标准差(s)", "最小(s)", "最大(s)", "吞吐 MiB/s", "通过率"],
            minio_summary_rows,
            row_classes=minio_summary_classes,
        ))
        sections.append("</div>")
        sections.append("<h3>MinIO 逐轮明细</h3>")
        minio_sample_rows: list[list[str]] = []
        for row in minio_io.get("samples") or []:
            minio_sample_rows.append([
                esc("写入" if row.get("operation") == "write" else "读取" if row.get("operation") == "read" else row.get("operation")),
                esc(row.get("object_type")),
                esc(row.get("round")),
                esc(row.get("tile_count")),
                esc(row.get("payload_bytes")),
                esc(row.get("total_bytes")),
                esc(row.get("io_sec")),
                esc(row.get("write_mib_per_sec") if row.get("operation") == "write" else row.get("read_mib_per_sec")),
                esc(row.get("verify_sec")),
                esc(row.get("cleanup_sec")),
                esc(row.get("source_object_count")),
                esc(row.get("valid")),
                esc(row.get("error") or "-"),
            ])
        sections.append('<div class="table-wrap">')
        sections.append(_html_table(
            ["操作", "对象类型", "轮次", "对象数", "平均单对象字节", "总字节", "I/O(s)", "吞吐 MiB/s", "校验(s)", "清理(s)", "读取对象池", "有效", "错误"],
            minio_sample_rows,
        ))
        sections.append("</div>")
        minio_phase_rows: list[list[str]] = []
        for row in minio_io.get("phases") or []:
            minio_phase_rows.append([
                esc("写入" if row.get("operation") == "write" else "读取" if row.get("operation") == "read" else row.get("operation")),
                esc(row.get("object_type")),
                esc(row.get("round")),
                esc(row.get("tile_count")),
                esc(row.get("phase")),
                esc(row.get("elapsed_sec")),
            ])
        sections.append("<details><summary>MinIO 阶段计时明细</summary>")
        sections.append('<div class="table-wrap">')
        sections.append(_html_table(["操作", "对象类型", "轮次", "对象数", "阶段", "耗时(s)"], minio_phase_rows))
        sections.append("</div></details>")

    sections.append("</main></body></html>")
    path.write_text("\n".join(sections), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else root
    output_dir.mkdir(parents=True, exist_ok=True)
    samples: list[dict[str, Any]] = []
    summary_paths = sorted(root.glob("round_*/**/summary.csv"))
    if not summary_paths:
        raise RuntimeError(f"no complete multi-round runs found under {root}")
    paths_by_round: dict[str, list[Path]] = {}
    for summary_path in summary_paths:
        round_name = summary_path.relative_to(root).parts[0]
        paths_by_round.setdefault(round_name, []).append(summary_path)
    for round_name, paths in sorted(paths_by_round.items()):
        if len(paths) != 1:
            raise RuntimeError(f"round {round_name} must contain exactly one run summary, found {len(paths)}")
        round_samples = load_run(paths[0].parent, root)
        case_ids = [str(row["case_id"]) for row in round_samples]
        if len(case_ids) != len(EXPECTED_CASE_IDS) or set(case_ids) != EXPECTED_CASE_IDS:
            raise RuntimeError(
                f"round {round_name} must contain exactly the 10 fixed cases; observed {sorted(set(case_ids))}"
            )
        samples.extend(round_samples)
    if not samples:
        raise RuntimeError(f"no complete multi-round runs found under {root}")
    aggregates = aggregate_rows(samples)
    input_metadata = load_input_profile(root)
    minio_io = load_minio_io_report(root)
    write_csv(output_dir / "multi_round_samples.csv", samples)
    write_csv(output_dir / "multi_round_summary.csv", aggregates)
    phase_output = output_dir / "multi_round_phase_timings.csv"
    phase_rows: list[dict[str, Any]] = []
    for summary_path in sorted(root.glob("round_*/**/phase_timings.csv")):
        run_dir = summary_path.parent
        round_name = run_dir.relative_to(root).parts[0]
        for row in load_phase_rows(summary_path):
            phase_rows.append({
                "round": round_name,
                "case_id": row.get("case_id", ""),
                "path": row.get("path", ""),
                "scope": row.get("scope", ""),
                "phase": row.get("phase", ""),
                "phase_count": row.get("phase_count", ""),
                "phase_elapsed_sec": row.get("phase_elapsed_sec", ""),
                "elapsed_sec": row.get("elapsed_sec", ""),
            })
    write_csv(phase_output, phase_rows)
    metadata = {
        "round_count": len({row["round"] for row in samples}),
        "sample_count": len(samples),
        "outlier_rule": "driver ray.wait/ray.wait_get > 10s and worker scope max < 6s",
        "excluded_sample_count": sum(bool(row["outlier"]) or not row["valid_sample"] for row in samples),
    }
    (output_dir / "multi_round_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(
        output_dir / "MULTI_ROUND_REPORT.md",
        root=root,
        samples=samples,
        aggregates=aggregates,
        input_metadata=input_metadata,
    )
    html_output = output_dir / "MULTI_ROUND_REPORT.html"
    write_html_report(
        html_output,
        root=root,
        samples=samples,
        aggregates=aggregates,
        phase_rows=phase_rows,
        input_metadata=input_metadata,
        minio_io=minio_io,
    )
    if minio_io:
        write_public_minio_artifacts(output_dir, minio_io)
    print(json.dumps({
        "report": str(output_dir / "MULTI_ROUND_REPORT.md"),
        "html_report": str(html_output),
        "summary": str(output_dir / "multi_round_summary.csv"),
        "samples": str(output_dir / "multi_round_samples.csv"),
        "phase_timings": str(output_dir / "multi_round_phase_timings.csv"),
        "round_count": metadata["round_count"],
        "sample_count": metadata["sample_count"],
        "excluded_sample_count": metadata["excluded_sample_count"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
