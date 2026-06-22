from __future__ import annotations

import argparse
from typing import Any

from cube_split.quality.optical_quality import run_quality_check as run_raster_quality_check


def run_quality_check(args: argparse.Namespace) -> dict[str, Any]:
    report = run_raster_quality_check(args)
    report["data_type"] = "radar"
    return report


def main() -> None:
    from cube_split.quality.optical_quality import main as optical_quality_main

    optical_quality_main()


if __name__ == "__main__":
    main()
