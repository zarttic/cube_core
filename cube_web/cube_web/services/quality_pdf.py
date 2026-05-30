from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from html import escape
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import Response


def _wrap_pdf_line(text: str, width: int = 96) -> list[str]:
    text = re.sub(r"\s+", " ", str(text)).strip()
    if not text:
        return [""]
    lines: list[str] = []
    while len(text) > width:
        split_at = text.rfind(" ", 0, width + 1)
        if split_at <= 0:
            split_at = width
        lines.append(text[:split_at])
        text = text[split_at:].strip()
    lines.append(text)
    return lines


def _quality_report_lines(report: dict, data_type: str) -> list[str]:
    summary = report.get("summary", {}) or {}
    data_type_texts = {
        "product": "数据产品",
        "carbon": "碳卫星",
        "carbon_satellite": "碳卫星",
    }
    data_type_text = data_type_texts.get(data_type, "光学遥感")
    lines = [
        "质检报告",
        "",
        "报告名称：质检报告",
        f"数据类型：{data_type_text}",
        f"质检状态：{report.get('status', 'UNKNOWN')}",
        f"目标参考系统：{report.get('target_crs', '-')}",
        f"生成时间：{report.get('generated_at', '-')}",
        f"批次目录：{report.get('run_dir', '-')}",
        "",
        "质检概要",
        f"- 索引行数：{summary.get('index_rows', 0)}",
        f"- 资产数量：{summary.get('asset_count', 0)}",
        f"- 通过项：{summary.get('passed_checks', 0)}",
        f"- 告警项：{summary.get('warning_checks', 0)}",
        f"- 失败项：{summary.get('failed_checks', 0)}",
    ]
    if summary.get("distinct_space_codes") is not None:
        lines.append(f"- 空间格网数：{summary.get('distinct_space_codes')}")
    if summary.get("distinct_st_codes") is not None:
        lines.append(f"- 时空编码数：{summary.get('distinct_st_codes')}")

    rows_by_band = summary.get("rows_by_band") or {}
    rows_by_year = summary.get("rows_by_year") or {}
    rows_by_satellite = summary.get("rows_by_satellite") or {}
    rows_by_product_type = summary.get("rows_by_product_type") or {}
    quality_counts = summary.get("quality_counts") or {}
    if rows_by_band:
        lines.extend(["", "波段行数"])
        lines.extend(f"- {band}: {value}" for band, value in sorted(rows_by_band.items()))
    if rows_by_year:
        lines.extend(["", "年份行数"])
        lines.extend(f"- {year}: {value}" for year, value in sorted(rows_by_year.items()))
    if rows_by_satellite:
        lines.extend(["", "卫星行数"])
        lines.extend(f"- {satellite}: {value}" for satellite, value in sorted(rows_by_satellite.items()))
    if rows_by_product_type:
        lines.extend(["", "产品类型行数"])
        lines.extend(f"- {product_type}: {value}" for product_type, value in sorted(rows_by_product_type.items()))
    if quality_counts:
        lines.extend(["", "质量标记分布"])
        lines.extend(f"- {flag}: {value}" for flag, value in sorted(quality_counts.items()))

    lines.extend(["", "检查项"])
    for check in report.get("checks", []) or []:
        lines.append(f"- [{check.get('status', 'UNKNOWN')}] {check.get('name', '-')}: {check.get('message', '')}")
        metrics = check.get("metrics") or {}
        for key, value in list(metrics.items())[:8]:
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            lines.extend(f"  {wrapped}" for wrapped in _wrap_pdf_line(f"{key}: {value}", width=90))

    assets = report.get("assets", []) or []
    if assets:
        lines.extend(["", "资产抽查"])
        for asset in assets[:12]:
            lines.append(f"- {Path(str(asset.get('path', '-'))).name} | 参考系统：{asset.get('crs', '-')}")
    return lines


def _quality_report_filename(report: dict, data_type: str, suffix: str) -> str:
    run_name = Path(str(report.get("run_dir", "run"))).name or "run"
    return f"quality-report-{data_type}-{run_name}.{suffix}"


def _quality_report_html(lines: list[str]) -> str:
    body_parts: list[str] = []
    for line in lines:
        if not line:
            body_parts.append("<div class='spacer'></div>")
        elif line == "质检报告":
            body_parts.append(f"<div class='title'>{escape(line)}</div>")
        elif line in {"质检概要", "波段行数", "年份行数", "卫星行数", "产品类型行数", "质量标记分布", "检查项", "资产抽查"}:
            body_parts.append(f"<h2>{escape(line)}</h2>")
        else:
            body_parts.append(f"<p>{escape(line)}</p>")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    @page {{ size: A4; margin: 18mm 16mm; }}
    body {{
      font-family: "Noto Sans CJK SC", "Source Han Sans SC", "WenQuanYi Zen Hei", sans-serif;
      color: #1f2937;
      font-size: 11pt;
      line-height: 1.55;
    }}
    .title {{
      font-size: 24pt;
      font-weight: 700;
      margin: 0 0 14pt;
      color: #12395b;
      border-bottom: 2pt solid #2d5f8a;
      padding-bottom: 8pt;
    }}
    h2 {{
      font-size: 14pt;
      margin: 14pt 0 7pt;
      color: #12395b;
    }}
    p {{
      margin: 3pt 0;
      word-break: break-all;
    }}
    .spacer {{ height: 6pt; }}
  </style>
</head>
<body>
  {''.join(body_parts)}
</body>
</html>"""


def _build_quality_report_pdf(lines: list[str]) -> bytes:
    libreoffice = shutil.which("libreoffice")
    if not libreoffice:
        raise HTTPException(status_code=500, detail="LibreOffice is required for PDF export")
    with tempfile.TemporaryDirectory(prefix="cube-web-quality-pdf-") as tmp:
        tmp_dir = Path(tmp)
        html_path = tmp_dir / "quality_report.html"
        profile_dir = tmp_dir / "lo-profile"
        runtime_dir = tmp_dir / "runtime"
        profile_dir.mkdir()
        runtime_dir.mkdir()
        html_path.write_text(_quality_report_html(lines), encoding="utf-8")
        env = os.environ.copy()
        env.update({"HOME": str(tmp_dir), "XDG_RUNTIME_DIR": str(runtime_dir)})
        result = subprocess.run(
            [
                libreoffice,
                "--headless",
                f"-env:UserInstallation=file://{profile_dir}",
                "--convert-to",
                "pdf:writer_web_pdf_Export",
                "--outdir",
                str(tmp_dir),
                str(html_path),
            ],
            cwd=tmp_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
            check=False,
        )
        pdf_path = tmp_dir / "quality_report.pdf"
        if result.returncode != 0 or not pdf_path.exists():
            detail = (result.stderr or result.stdout or "PDF conversion failed").strip()
            raise HTTPException(status_code=500, detail=detail)
        return pdf_path.read_bytes()


def quality_report_pdf_response(report: dict, data_type: str) -> Response:
    pdf = _build_quality_report_pdf(_quality_report_lines(report, data_type))
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{_quality_report_filename(report, data_type, "pdf")}"'},
    )


def quality_report_text_response(report: dict, data_type: str) -> Response:
    text = "\n".join(_quality_report_lines(report, data_type)).rstrip() + "\n"
    return Response(
        content=text,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{_quality_report_filename(report, data_type, "txt")}"'},
    )
