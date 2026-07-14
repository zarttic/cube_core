"""Generate authoritative ISEA4H fixture vectors using DGGRID v8.44.

This tool shells out to a locally built DGGRID binary to produce the ground
truth for the pure-Python ISEA4H engine. It captures, per resolution:
  - cell centers   : seqnum -> (lon, lat)     [GLOBAL_SEQUENCE ordering]
  - neighbors      : seqnum -> [neighbor seqnums]
  - children       : seqnum -> [child seqnums at res+1]

DGGRID configuration is the frozen ISEA4H baseline:
  dggs_type ISEA4H, dggs_proj ISEA, dggs_topology HEXAGON,
  dggs_aperture 4 (PURE), orientation (11.25, 58.28252559, 0.0),
  proj_datum WGS84_AUTHALIC_SPHERE, output_address_type SEQNUM.

Runtime code MUST NOT depend on this tool or on DGGRID. Only the committed
JSONL fixtures under tests/fixtures/isea4h/ are used by the engine tests.

Usage:
  DGGRID_BIN=/path/to/dggrid python3.11 cube_encoder/tools/generate_isea4h_vectors.py \
    --max-res 6 --topo-max-res 4
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "isea4h"

META_HEADER = """dggrid_operation GENERATE_GRID
dggs_type ISEA4H
dggs_proj ISEA
dggs_topology HEXAGON
dggs_aperture_type PURE
dggs_aperture 4
dggs_orient_specify_type SPECIFIED
dggs_vert0_lon 11.25
dggs_vert0_lat 58.28252559
dggs_vert0_azimuth 0.0
proj_datum WGS84_AUTHALIC_SPHERE
dggs_res_specify_type SPECIFIED
dggs_res_spec {res}
clip_subset_type WHOLE_EARTH
output_cell_label_type OUTPUT_ADDRESS_TYPE
output_address_type SEQNUM
"""


def _dggrid_bin() -> str:
    candidates = [
        os.environ.get("DGGRID_BIN", ""),
        str(Path(__file__).resolve().parent.parent.parent / ".dggrid_src" / "build" / "src" / "apps" / "dggrid" / "dggrid"),
    ]
    for cand in candidates:
        if cand and Path(cand).is_file() and os.access(cand, os.X_OK):
            return cand
    raise SystemExit(
        "DGGRID binary not found. Set DGGRID_BIN or build it under .dggrid_src/build."
    )


def _run_dggrid(meta_text: str, workdir: Path) -> None:
    meta_path = workdir / "run.meta"
    meta_path.write_text(meta_text)
    result = subprocess.run(
        [_dggrid_bin(), str(meta_path)],
        cwd=str(workdir),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"DGGRID failed:\n{result.stdout}\n{result.stderr}")


def _centers(res: int, workdir: Path) -> dict[int, tuple[float, float]]:
    """seqnum -> (lon, lat) via TEXT point output."""
    meta = META_HEADER.format(res=res) + (
        "point_output_type TEXT\n"
        "point_output_file_name points\n"
        "cell_output_type NONE\n"
    )
    _run_dggrid(meta, workdir)
    centers: dict[int, tuple[float, float]] = {}
    for line in (workdir / "points.txt").read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        seq_str, lon_str, lat_str = line.split(",")
        centers[int(seq_str)] = (round(float(lon_str), 7), round(float(lat_str), 7))
    return centers


def _neighbors(res: int, workdir: Path) -> dict[int, list[int]]:
    """seqnum -> sorted neighbor seqnums via TEXT neighbor output."""
    meta = META_HEADER.format(res=res) + (
        "point_output_type NONE\n"
        "cell_output_type NONE\n"
        "neighbor_output_type TEXT\n"
        "neighbor_output_file_name nbr\n"
    )
    _run_dggrid(meta, workdir)
    out: dict[int, list[int]] = {}
    for line in (workdir / "nbr.nbr").read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        out[int(parts[0])] = sorted(int(p) for p in parts[1:])
    return out


def _children(res: int, workdir: Path) -> dict[int, list[int]]:
    """seqnum(res) -> sorted child seqnums(res+1) via TEXT children output."""
    meta = META_HEADER.format(res=res) + (
        "point_output_type NONE\n"
        "cell_output_type NONE\n"
        "children_output_type TEXT\n"
        "children_output_file_name chd\n"
    )
    _run_dggrid(meta, workdir)
    out: dict[int, list[int]] = {}
    for line in (workdir / "chd.chd").read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        out[int(parts[0])] = sorted(int(p) for p in parts[1:])
    return out


def generate(max_res: int, topo_max_res: int) -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for res in range(max_res + 1):
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            centers = _centers(res, workdir)
            rows = [
                {"res": res, "seqnum": str(s), "center_lon": lon, "center_lat": lat}
                for s, (lon, lat) in sorted(centers.items())
            ]
            out_path = FIXTURE_DIR / f"vectors_res{res}.jsonl"
            out_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
            print(f"res {res}: {len(rows)} centers -> {out_path.name}")

    for res in range(topo_max_res + 1):
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            nbrs = _neighbors(res, workdir)
            (FIXTURE_DIR / f"neighbors_res{res}.jsonl").write_text(
                "\n".join(
                    json.dumps({"res": res, "seqnum": str(s), "neighbors": [str(n) for n in ns]})
                    for s, ns in sorted(nbrs.items())
                )
                + "\n"
            )
            print(f"res {res}: {len(nbrs)} neighbor rows")

    for res in range(topo_max_res):
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            chd = _children(res, workdir)
            (FIXTURE_DIR / f"children_res{res}.jsonl").write_text(
                "\n".join(
                    json.dumps({"res": res, "seqnum": str(s), "children": [str(c) for c in cs]})
                    for s, cs in sorted(chd.items())
                )
                + "\n"
            )
            print(f"res {res}: {len(chd)} children rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-res", type=int, default=6, help="max resolution for centers")
    ap.add_argument("--topo-max-res", type=int, default=4, help="max resolution for neighbors/children")
    args = ap.parse_args()
    generate(args.max_res, args.topo_max_res)

