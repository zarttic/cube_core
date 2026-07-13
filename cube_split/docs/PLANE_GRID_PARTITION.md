# Plane Grid Logical Partition

状态：当前实现说明（2026-07-13）。这是源平面逻辑窗口的局部契约，不是全局拓扑设计。

`plane_grid` is the source-plane grid for rasters that already use a projected or otherwise native planar CRS.
It cuts each source asset by pixel windows in the asset CRS and does not default to `EPSG:4326` reprojection.

## How It Differs

- `isea4h`: global discrete hexagon grid. Use it for entity partition output and hex coverage.
- `tile_matrix`: longitude/latitude matrix grid. Coverage still goes through WGS84 bbox and encoder topology.
- `s2`: global cell grid. Coverage still goes through WGS84 bbox and encoder topology.
- `plane_grid`: source CRS/pixel grid. Windows are generated from raster width, height, transform, and CRS.

## Space Code And Level

`plane_grid` space codes use:

```text
<crs_token>/<level>/<col>/<row>
```

Example:

```text
epsg32650/11/3/4
```

The ST code prefix is `pg`, so the example above with day granularity becomes:

```text
pg:11:epsg32650/11/3/4:20260309
```

The level maps to pixel chunk size:

```text
chunk_pixels = 2 ** max(0, 13 - level)
```

For example, level 5 is 256x256 pixels and level 11 is 4x4 pixels.

## Runtime Behavior

- `target_crs` must be empty for `plane_grid`; this preserves the source CRS.
- `max_cells_per_asset=0` disables the safety limit and is the production default.
- Output rows keep existing `cell_min_lon/cell_min_lat/...` fields for ingest compatibility, but for `plane_grid` these values are source CRS x/y bounds. Native coordinates are also emitted as `cell_min_x/cell_min_y/cell_max_x/cell_max_y` with `cell_crs`.
- Output rows also include `cell_crs`, `cell_min_x`, `cell_min_y`, `cell_max_x`, `cell_max_y`, and source window fields.

## Limits

- `plane_grid` is implemented for logical partition only.
- Entity partition remains on encoder-backed `s2`, `tile_matrix`, or `isea4h` cells.
- The grid is asset-local by CRS token, level, row, and column. It is optimized for source-aligned slicing, not global cross-scene topology.
- `CubeEncoderSDK.cover()` and the Web map preview do not support `plane_grid`; do not call the generic WGS84 cover endpoint for this mode.
- Current quality checks and ingestion uniqueness keys still assume WGS84/global-cell semantics. Treat multi-scene `plane_grid` ingestion as experimental until the planned origin/CRS-aware redesign lands.
