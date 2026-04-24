from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from cube_split.jobs.ray_partition_core import AssetRecord, convert_assets_to_cog


def test_convert_assets_to_cog_creates_cog_files(tmp_path: Path):
    with rasterio.Env() as env:
        if "COG" not in env.drivers():
            pytest.skip("COG driver unavailable in current GDAL build")

    source = tmp_path / "LC08_L2SP_123033_20201225_02_T1_blue.TIF"
    transform = from_origin(116.0, 40.0, 0.01, 0.01)
    data = np.ones((1, 8, 8), dtype=np.uint8)
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data)

    assets = [
        AssetRecord(
            scene_id="LC08_L2SP_123033_20201225_02_T1",
            band="blue",
            path=str(source),
            acq_time="2020-12-25T00:00:00Z",
        )
    ]
    converted = convert_assets_to_cog(assets, cog_input_dir=tmp_path / "cog", overwrite=False)

    assert len(converted) == 1
    out_path = Path(converted[0].path)
    assert out_path.exists()
    assert out_path.name.endswith("_cog.tif")
    assert out_path != source
    with rasterio.open(out_path) as ds:
        assert str(ds.profile.get("compress", "")).lower() == "lzw"
        assert ds.overviews(1) == []
