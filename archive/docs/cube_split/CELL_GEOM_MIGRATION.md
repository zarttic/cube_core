# `rs_cube_cell_fact.cell_geom` migration

`cell_geom` stores the SDK-computed WGS84 boundary of each cube cell. Geohash
rows contain a closed four-corner ring (5 points), and ISEA4H rows contain the
actual closed six-corner ISEA4H ring (7 points). Regular MGRS cells are reduced
to a closed four-corner ring only when the relative area error is at most
`1e-4`; cells clipped by a UTM/UPS domain or latitude-band boundary retain the
SDK's full closed Polygon so the stored geometry does not lose area or overlap
an adjacent domain. It is not derived from the row bbox and does not use H3.

The migration is additive. It neither drops nor renames the legacy table or any
existing column. Preview the affected distinct cells first:

```bash
PYTHONPATH=cube_encoder:cube_split python3.11 -m cube_split.scripts.migrate_cube_cell_geom
```

Apply and validate in one transaction:

```bash
PYTHONPATH=cube_encoder:cube_split python3.11 -m cube_split.scripts.migrate_cube_cell_geom --execute
```

The DSN is resolved through the normal `CUBE_WEB_POSTGRES_DSN` runtime chain.
The script rolls back if an existing code cannot produce a valid closed Polygon
or if the SRID is not 4326. Rollback of
a committed migration is intentionally non-destructive: retain the nullable
column and stop populating it rather than deleting historical geometry/data.
