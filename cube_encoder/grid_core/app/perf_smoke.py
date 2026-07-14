from __future__ import annotations

import json
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService


@dataclass(frozen=True)
class PerfCase:
    name: str
    iterations: int
    max_avg_ms: float
    func: Callable[[], None]


def _bench(case: PerfCase) -> tuple[float, float]:
    # Warmup to reduce one-time import/cache noise.
    case.func()
    costs = []
    for _ in range(case.iterations):
        start = time.perf_counter()
        case.func()
        costs.append((time.perf_counter() - start) * 1000.0)
    return statistics.mean(costs), max(costs)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def run_perf_smoke(enforce: bool = True) -> dict[str, dict[str, float]]:
    grid_service = GridService()
    topology_service = TopologyService()

    geohash_address = grid_service.locate_space_code(GridType.GEOHASH, 6, [116.391, 39.907])
    mgrs_address = grid_service.locate_space_code(GridType.MGRS, 5, [116.391, 39.907])
    isea_address = grid_service.locate_space_code(GridType.ISEA4H, 7, [116.391, 39.907])
    geohash_neighbors = topology_service.neighbors(geohash_address, k=1)[:8]

    polygon = {
        "type": "Polygon",
        "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911], [116.385, 39.911], [116.385, 39.903]]],
    }

    cases = [
        PerfCase(
            name="geohash_locate",
            iterations=2000,
            max_avg_ms=_env_float("PERF_MAX_GEOHASH_LOCATE_MS", 1.5),
            func=lambda: grid_service.locate(GridType.GEOHASH, 6, [116.391, 39.907]),
        ),
        PerfCase(
            name="mgrs_locate",
            iterations=4000,
            max_avg_ms=_env_float("PERF_MAX_MGRS_LOCATE_MS", 8.0),
            func=lambda: grid_service.locate(GridType.MGRS, 5, [116.391, 39.907]),
        ),
        PerfCase(
            name="isea4h_locate",
            iterations=2000,
            max_avg_ms=_env_float("PERF_MAX_ISEA4H_LOCATE_MS", 3.0),
            func=lambda: grid_service.locate(GridType.ISEA4H, 7, [116.391, 39.907]),
        ),
        PerfCase(
            name="geohash_cover_intersect",
            iterations=1000,
            max_avg_ms=_env_float("PERF_MAX_GEOHASH_COVER_MS", 80.0),
            func=lambda: grid_service.cover(
                GridType.GEOHASH,
                6,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                boundary_type=BoundaryType.BBOX,
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="geohash_cover_compact_intersect",
            iterations=1000,
            max_avg_ms=_env_float("PERF_MAX_GEOHASH_COMPACT_COVER_MS", 60.0),
            func=lambda: grid_service.cover_compact(
                GridType.GEOHASH,
                6,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="mgrs_cover_intersect",
            iterations=500,
            max_avg_ms=_env_float("PERF_MAX_MGRS_COVER_MS", 180.0),
            func=lambda: grid_service.cover(
                GridType.MGRS,
                3,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                boundary_type=BoundaryType.BBOX,
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="mgrs_cover_compact_intersect",
            iterations=500,
            max_avg_ms=_env_float("PERF_MAX_MGRS_COMPACT_COVER_MS", 150.0),
            func=lambda: grid_service.cover_compact(
                GridType.MGRS,
                3,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="isea4h_cover_intersect",
            iterations=200,
            max_avg_ms=_env_float("PERF_MAX_ISEA4H_COVER_MS", 300.0),
            func=lambda: grid_service.cover(
                GridType.ISEA4H,
                7,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                boundary_type=BoundaryType.BBOX,
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="isea4h_cover_compact_intersect",
            iterations=200,
            max_avg_ms=_env_float("PERF_MAX_ISEA4H_COMPACT_COVER_MS", 300.0),
            func=lambda: grid_service.cover_compact(
                GridType.ISEA4H,
                7,
                geometry=polygon,
                bbox=None,
                cover_mode="intersect",
                crs="EPSG:4326",
            ),
        ),
        PerfCase(
            name="topology_batch_geometries_20",
            iterations=2000,
            max_avg_ms=_env_float("PERF_MAX_BATCH_GEOMETRY_MS", 35.0),
            func=lambda: topology_service.codes_to_geometries(
                geohash_neighbors, BoundaryType.POLYGON
            ),
        ),
        PerfCase(
            name="topology_neighbors_mgrs",
            iterations=3000,
            max_avg_ms=_env_float("PERF_MAX_MGRS_NEIGHBORS_MS", 12.0),
            func=lambda: topology_service.neighbors(mgrs_address, k=1),
        ),
        PerfCase(
            name="topology_neighbors_isea4h",
            iterations=1000,
            max_avg_ms=_env_float("PERF_MAX_ISEA4H_NEIGHBORS_MS", 5.0),
            func=lambda: topology_service.neighbors(isea_address, k=1),
        ),
    ]

    results: dict[str, dict[str, float]] = {}
    violations: list[str] = []
    for case in cases:
        avg_ms, max_ms = _bench(case)
        results[case.name] = {
            "avg_ms": round(avg_ms, 3),
            "max_ms": round(max_ms, 3),
            "threshold_ms": case.max_avg_ms,
        }
        if avg_ms > case.max_avg_ms:
            violations.append(f"{case.name}: avg {avg_ms:.3f}ms > {case.max_avg_ms:.3f}ms")

    if enforce and violations:
        details = "; ".join(violations)
        raise RuntimeError(f"Perf smoke check failed: {details}")

    return results


def main() -> None:
    results = run_perf_smoke(enforce=True)
    json_path = os.getenv("PERF_SMOKE_JSON_PATH")
    if json_path:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "results": results,
        }
        target = Path(json_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print("Performance smoke report:")
    for name, row in results.items():
        print(
            f"- {name}: avg={row['avg_ms']:.3f}ms, max={row['max_ms']:.3f}ms, "
            f"threshold={row['threshold_ms']:.3f}ms"
        )


if __name__ == "__main__":
    main()
