from __future__ import annotations

from cube_split.ingest import dataset_cleanup as cleanup


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._rows: list[dict[str, object]] = []
        self.rowcount = 0
        self.closed = False

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.closed = True

    def execute(self, statement: str, params: tuple[object, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self._conn.executed.append((normalized, params))
        self._rows = self._conn.results_for(normalized)
        self.rowcount = self._conn.rowcount_for(normalized)

    def fetchall(self) -> list[dict[str, object]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None


class _Conn:
    """Fake connection: records SQL and returns canned catalog/selector rows."""

    def __init__(
        self,
        *,
        tables: tuple[str, ...] = (
            "rs_raw_scene_asset",
            "rs_entity_tile_asset",
            "rs_product_asset",
            "rs_product_cell_fact",
            "rs_cube_cell_fact",
            "rs_carbon_observation_fact",
            "rs_ingest_job",
        ),
        object_uris: tuple[str, ...] = (),
        delete_counts: dict[str, int] | None = None,
    ) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self._tables = tables
        self._object_uris = object_uris
        self.delete_counts = delete_counts or {}

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self)

    def results_for(self, statement: str) -> list[dict[str, object]]:
        if "FROM pg_class" in statement:
            return [{"relname": table} for table in self._tables]
        if "AS object_uri" in statement:
            return [{"object_uri": uri} for uri in self._object_uris]
        return []

    def rowcount_for(self, statement: str) -> int:
        if not statement.upper().startswith("DELETE"):
            return 0
        for marker, count in self.delete_counts.items():
            if marker in statement:
                return count
        return 1

    def deletes(self) -> list[tuple[str, tuple[object, ...]]]:
        return [entry for entry in self.executed if entry[0].startswith("DELETE")]


def test_make_ingest_job_id_matches_the_stored_value() -> None:
    # Golden value: pinned so the id cannot drift away from rows written at ingest
    # time by an older release (cube_web used to inline this expression).
    assert cleanup.make_ingest_job_id("dataset-a", "output-v1", "band-1") == "ingest-bd0de0c542e9d08160a3399a"
    assert cleanup.make_ingest_job_id("dataset-a", "output-v1", "band-2") != cleanup.make_ingest_job_id(
        "dataset-a", "output-v2", "band-1"
    )


def test_dataset_scoped_tables_delete_by_dataset_column() -> None:
    conn = _Conn()
    result = cleanup.delete_managed_dataset_rows(conn, dataset_id="dataset-a")

    deleted = {statement.split()[2]: params for statement, params in conn.deletes()}
    for table in cleanup.DATASET_COLUMN_TABLES:
        assert deleted[table] == ("dataset-a",)
    # No run/version identifiers were supplied, so the run-scoped tables stay untouched.
    assert "rs_cube_cell_fact" not in deleted
    assert "rs_carbon_observation_fact" not in deleted
    assert "rs_ingest_job" not in deleted
    assert result["deleted_total"] == 4


def test_run_scoped_tables_delete_by_run_id_and_own_version() -> None:
    conn = _Conn()
    result = cleanup.delete_managed_dataset_rows(
        conn,
        dataset_id="dataset-a",
        run_ids=["ingest-aaa", "ingest-bbb"],
        output_versions=["output-v1"],
    )

    statements = {statement: params for statement, params in conn.deletes()}
    cube = next(statement for statement in statements if "rs_cube_cell_fact" in statement)
    assert "run_id = ANY(%s::text[]) OR cube_version = ANY(%s::text[])" in cube
    assert statements[cube] == (["ingest-aaa", "ingest-bbb"], ["output-v1"])

    carbon = next(statement for statement in statements if "rs_carbon_observation_fact" in statement)
    assert statements[carbon] == (["ingest-aaa", "ingest-bbb"],)

    jobs = next(statement for statement in statements if "rs_ingest_job" in statement)
    assert statements[jobs] == (["ingest-aaa", "ingest-bbb"],)
    assert result["deleted_rows"]["rs_cube_cell_fact"] > 0


def test_missing_tables_are_skipped_instead_of_failing() -> None:
    conn = _Conn(tables=("rs_cube_cell_fact",))
    cleanup.delete_managed_dataset_rows(
        conn, dataset_id="dataset-a", run_ids=["ingest-aaa"], output_versions=["output-v1"]
    )
    deleted_tables = [statement.split()[2] for statement, _ in conn.deletes()]
    assert deleted_tables == ["rs_cube_cell_fact"]


def test_only_ingest_created_objects_are_reported_for_cleanup() -> None:
    conn = _Conn(
        object_uris=(
            "s3://cube/cube/raw/dataset=dataset-a/sensor=s1/version=v1/a.tif",  # ingest copy, owned
            "s3://cube/partition/dataset-a/versions/output-v1/tiles/1.tif",  # partition output, owned
            "s3://cube/cube/raw/dataset=dataset-b/sensor=s1/version=v1/b.tif",  # other dataset
            "s3://cube/cube/source/product/输入产品.tif",  # loader-owned source
            "s3://cube/cube/source/x.tif#window=0,0,8,8",  # window reference, never delete
        )
    )
    result = cleanup.delete_managed_dataset_rows(conn, dataset_id="dataset-a")

    assert result["object_uris"] == [
        "s3://cube/cube/raw/dataset=dataset-a/sensor=s1/version=v1/a.tif",
        "s3://cube/partition/dataset-a/versions/output-v1/tiles/1.tif",
    ]


def test_source_data_objects_are_never_reported_even_with_matching_prefix() -> None:
    """源数据保护：即使用户桶/源目录里出现 dataset=<id> 形状，也绝不回传删除。"""
    conn = _Conn(
        object_uris=(
            # 用户上传的原始影像（源），路径里恰好带 dataset= 形状 —— 必须跳过
            "s3://user-1/datas/dataset=dataset-a/product/LANDSAT.tif",
            # 载入子系统的 shared_delivery 目录
            "s3://cube/shared_delivery_ard/dataset=dataset-a/raw.tif",
            # 源目录前缀
            "s3://cube/cube/source/optical/dataset=dataset-a/x.tif",
            # 对照：真正的 ingest 副本（cube 桶 + cube/raw 前缀，见 ray_ingest_job 的 --minio-prefix 默认值）
            "s3://cube/cube/raw/dataset=dataset-a/sensor=s1/version=v1/a.tif",
        )
    )
    result = cleanup.delete_managed_dataset_rows(conn, dataset_id="dataset-a")

    assert result["object_uris"] == ["s3://cube/cube/raw/dataset=dataset-a/sensor=s1/version=v1/a.tif"]


def test_helper_never_commits_or_deletes_without_a_scope() -> None:
    conn = _Conn(tables=("rs_cube_cell_fact", "rs_ingest_job"))
    cleanup.delete_managed_dataset_rows(conn, dataset_id="dataset-a")

    assert [statement for statement, _ in conn.executed if statement.upper().startswith(("COMMIT", "ROLLBACK"))] == []
    assert conn.deletes() == []


def test_managed_table_cleanups_describe_every_owned_delete_in_order() -> None:
    cleanups = cleanup.managed_table_cleanups(
        "dataset-a", run_ids=["ingest-1"], output_versions=["ov-1", "ov-2"]
    )

    assert [(item.table, item.predicate) for item in cleanups] == [
        ("rs_raw_scene_asset", "dataset = %s"),
        ("rs_entity_tile_asset", "dataset = %s"),
        ("rs_product_asset", "dataset = %s"),
        ("rs_product_cell_fact", "dataset = %s"),
        ("rs_cube_cell_fact", "run_id = ANY(%s::text[]) OR cube_version = ANY(%s::text[])"),
        ("rs_carbon_observation_fact", "run_id = ANY(%s::text[])"),
        ("rs_ingest_job", "job_id = ANY(%s::text[])"),
    ]
    assert cleanups[0].params == ("dataset-a",)
    assert cleanups[4].params == (["ingest-1"], ["ov-1", "ov-2"])
    assert cleanups[0].object_uri_column == "raw_cog_uri"
    assert cleanups[1].object_uri_column == "tile_uri"
    assert cleanups[3].object_uri_column is None


def test_managed_table_cleanups_skip_statements_without_a_scope() -> None:
    without_versions = cleanup.managed_table_cleanups("dataset-a", run_ids=["ingest-1"])
    assert [item.table for item in without_versions] == [
        "rs_raw_scene_asset",
        "rs_entity_tile_asset",
        "rs_product_asset",
        "rs_product_cell_fact",
        "rs_cube_cell_fact",
        "rs_carbon_observation_fact",
        "rs_ingest_job",
    ]
    assert without_versions[4].params == (["ingest-1"],)

    only_versions = cleanup.managed_table_cleanups("dataset-a", output_versions=["ov-1"])
    assert [item.table for item in only_versions][-2:] == ["rs_product_cell_fact", "rs_cube_cell_fact"]
    assert only_versions[-1].predicate == "cube_version = ANY(%s::text[])"


def test_delete_table_batch_deletes_at_most_one_batch() -> None:
    conn = _Conn(delete_counts={"DELETE FROM rs_product_cell_fact": 3})

    deleted = cleanup.delete_table_batch(
        conn,
        table="rs_product_cell_fact",
        predicate="dataset = %s",
        params=("dataset-a",),
        batch_size=500,
    )

    assert deleted == 3
    statement, params = conn.deletes()[-1]
    assert statement == (
        "DELETE FROM rs_product_cell_fact WHERE ctid IN "
        "(SELECT ctid FROM rs_product_cell_fact WHERE dataset = %s LIMIT 500)"
    )
    assert params == ("dataset-a",)


def test_delete_table_batch_rejects_a_non_positive_batch_size() -> None:
    conn = _Conn()
    for batch_size in (0, -1):
        try:
            cleanup.delete_table_batch(
                conn, table="rs_ingest_job", predicate="job_id = ANY(%s::text[])", params=(["x"],), batch_size=batch_size
            )
        except ValueError as exc:
            assert "positive" in str(exc)
        else:  # pragma: no cover - guard must raise
            raise AssertionError("batch_size must be validated")
    assert conn.deletes() == []


def test_existing_managed_tables_only_asks_the_catalog_about_contract_tables() -> None:
    conn = _Conn(tables=("rs_product_asset",))

    assert cleanup.existing_managed_tables(conn) == {"rs_product_asset"}
    statement, params = conn.executed[-1]
    assert "FROM pg_class" in statement
    assert set(params[0]) == {
        "rs_raw_scene_asset",
        "rs_entity_tile_asset",
        "rs_product_asset",
        "rs_product_cell_fact",
        "rs_cube_cell_fact",
        "rs_carbon_observation_fact",
        "rs_ingest_job",
    }
    # Nothing outside the rs_* contract may ever reach a DELETE statement.
    assert not [name for name in params[0] if not name.startswith("rs_")]
    assert [statement for statement, _ in conn.deletes()] == []
