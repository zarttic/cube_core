# `rs_cube_cell_fact.cell_geom` migration

`cell_geom` stores the SDK-computed WGS84 boundary of each cube cell. Geohash
and MGRS rows contain a closed four-corner ring (5 points); ISEA4H rows contain
the actual closed six-corner ISEA4H ring (7 points). It is not derived from the
row bbox and does not use H3.

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
The script rolls back if an existing code cannot produce a valid Polygon, if
the SRID is not 4326, or if a ring has the wrong number of points. Rollback of
a committed migration is intentionally non-destructive: retain the nullable
column and stop populating it rather than deleting historical geometry/data.
