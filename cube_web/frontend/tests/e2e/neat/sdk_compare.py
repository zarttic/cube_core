#!/usr/bin/env python3.11
"""Compare the /encoding page's captured request/response with the CubeEncoderSDK result.

Usage: sdk_compare.py encoding-preview.json
Prints JSON: {grid_type, requested_grid_level, point/bbox, ui_codes, sdk_codes, match}
"""
from __future__ import annotations

import json
import sys

from grid_core.sdk import CubeEncoderSDK


def main(path: str) -> int:
    with open(path, encoding="utf8") as handle:
        captures = json.load(handle)
    sdk = CubeEncoderSDK()
    results = []
    for capture in captures:
        url = capture.get("url", "")
        request = capture.get("request") or {}
        response = capture.get("response") or {}
        grid_type = request.get("grid_type")
        level = request.get("requested_grid_level")
        if not grid_type:
            continue
        entry = {"url": url.split("?")[0], "grid_type": grid_type, "level": level}
        if "/locate" in url:
            point = request.get("point")
            cell = sdk.locate(grid_type=grid_type, requested_grid_level=int(level), point=tuple(point))
            entry["point"] = point
            entry["ui_codes"] = [response.get("cell", {}).get("space_code")]
            entry["sdk_codes"] = [cell.space_code]
        else:
            bbox = request.get("bbox")
            cells = sdk.cover(grid_type=grid_type, requested_grid_level=int(level), bbox=tuple(bbox), cover_mode="intersect", boundary_type="polygon", crs="EPSG:4326")
            entry["bbox"] = bbox
            entry["ui_codes"] = [cell.get("space_code") for cell in (response.get("cells") or [])][:50]
            entry["sdk_codes"] = [cell.space_code for cell in cells][:50]
            entry["ui_count"] = response.get("statistics", {}).get("cell_count") or len(response.get("cells") or [])
            entry["sdk_count"] = len(cells)
        entry["match"] = entry["ui_codes"] == entry["sdk_codes"]
        results.append(entry)
    print(json.dumps({"checks": results}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
