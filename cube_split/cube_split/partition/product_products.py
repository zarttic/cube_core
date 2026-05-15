from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ProductAssetMetadata:
    scene_id: str
    band: str
    acq_time: datetime
    product_family: str
    sensor: str
    product_name: str
    product_year: int


_YEAR_RE = re.compile(r"_(\d{4})年$", re.IGNORECASE)


def parse_product_asset(path: Path) -> ProductAssetMetadata:
    match = _YEAR_RE.search(path.stem)
    if not match:
        raise ValueError(f"Product TIF filename must end with _YYYY年: {path.name}")
    year = int(match.group(1))
    product_name = path.stem[: match.start()]
    scene_id = f"dianzhong_ecological_security_{year}"
    return ProductAssetMetadata(
        scene_id=scene_id,
        band="product_value",
        acq_time=datetime(year, 1, 1, tzinfo=timezone.utc),
        product_family="product",
        sensor="data_product",
        product_name=product_name,
        product_year=year,
    )
