from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Iterator
from uuid import uuid4

from cube_web.services.partition_contracts import BandInput, DatasetInput, SourceAssetInput
from cube_web.services.scene_contracts import ScenePartitionRunRequest


class OpenGaussSceneRepository:
    def __init__(self, dsn: str | None, *, connection_factory: Any | None = None) -> None:
        self.dsn = dsn
        self.connection_factory = connection_factory

    def upsert_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]:
        load_batch_id = str(payload.get("load_batch_id") or "").strip()
        if not load_batch_id:
            raise ValueError("load_batch_id is required")
        datasets = _load_schema_datasets(payload, load_batch_id=load_batch_id)
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    MERGE INTO load_batches target USING (
                      SELECT %s::text AS load_batch_id, %s::text AS batch_name,
                             %s::text AS source_system, %s::jsonb AS attributes,
                             %s::timestamptz AS loaded_at
                    ) source ON (target.load_batch_id = source.load_batch_id)
                    WHEN MATCHED THEN UPDATE SET
                      batch_name = source.batch_name, source_system = source.source_system,
                      attributes = target.attributes || source.attributes,
                      loaded_at = COALESCE(source.loaded_at, target.loaded_at), updated_at = now()
                    WHEN NOT MATCHED THEN INSERT (
                      load_batch_id, batch_name, source_system, status, attributes, loaded_at
                    ) VALUES (
                      source.load_batch_id, source.batch_name, source.source_system,
                      'pending', source.attributes, source.loaded_at
                    )
                    """,
                    (
                        load_batch_id,
                        str(payload.get("batch_name") or load_batch_id),
                        str(payload.get("source_system") or "loader"),
                        json.dumps({"schema_version": payload.get("schema_version") or "1.0"}),
                        payload.get("loaded_at"),
                    ),
                )
                scene_count = 0
                for dataset in datasets:
                    cursor.execute(
                        """
                        MERGE INTO datasets target USING (
                          SELECT %s::text AS dataset_id, %s::text AS dataset_code,
                                 %s::text AS dataset_title, %s::text AS data_type,
                                 %s::text AS product_type, %s::text AS status,
                                 %s::numeric AS assignment_confidence, %s::text AS assignment_issue,
                                 %s::boolean AS auto_ingest_allowed, %s::jsonb AS attributes
                        ) source ON (target.dataset_id = source.dataset_id)
                        WHEN MATCHED THEN UPDATE SET
                          dataset_code = source.dataset_code, dataset_title = source.dataset_title,
                          data_type = source.data_type, product_type = source.product_type,
                          assignment_confidence = source.assignment_confidence,
                          assignment_issue = source.assignment_issue,
                          auto_ingest_allowed = source.auto_ingest_allowed,
                          attributes = target.attributes || source.attributes, updated_at = now()
                        WHEN NOT MATCHED THEN INSERT (
                          dataset_id,dataset_code,dataset_title,data_type,product_type,status,
                          assignment_confidence,assignment_issue,auto_ingest_allowed,attributes
                        ) VALUES (
                          source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,
                          source.product_type,source.status,source.assignment_confidence,
                          source.assignment_issue,source.auto_ingest_allowed,source.attributes
                        )
                        """,
                        (
                            dataset["dataset_id"],
                            dataset["dataset_code"],
                            dataset["dataset_title"],
                            dataset["data_type"],
                            dataset.get("product_type"),
                            dataset["status"],
                            dataset["assignment_confidence"],
                            dataset.get("assignment_issue"),
                            dataset["auto_ingest_allowed"],
                            json.dumps(dataset["attributes"]),
                        ),
                    )
                    for scene in dataset["scenes"]:
                        cursor.execute(
                            "SELECT scene_id,dataset_id,checksum FROM scenes WHERE identity_key = %s FOR UPDATE",
                            (scene["identity_key"],),
                        )
                        identity_row = cursor.fetchone()
                        if identity_row is not None:
                            stored_checksum = str(identity_row[2] or "")
                            incoming_checksum = str(scene.get("checksum") or "")
                            if stored_checksum and incoming_checksum and stored_checksum != incoming_checksum:
                                raise ValueError(f"scene identity checksum conflict: {scene['scene_key']}")
                        cursor.execute(
                            """
                            MERGE INTO scenes target USING (
                              SELECT %s::text AS scene_id, %s::text AS dataset_id,
                                     %s::text AS scene_key, %s::text AS identity_key,
                                     %s::text AS source_asset_id, %s::text AS source_uri,
                                     %s::text AS checksum, %s::timestamptz AS acquisition_time,
                                     %s::jsonb AS bbox, %s::text AS crs, %s::jsonb AS attributes
                            ) source ON (target.identity_key = source.identity_key)
                            WHEN MATCHED THEN UPDATE SET
                              source_asset_id = source.source_asset_id, source_uri = source.source_uri,
                              checksum = source.checksum, acquisition_time = source.acquisition_time,
                              bbox = source.bbox, crs = source.crs,
                              attributes = target.attributes || source.attributes, updated_at = now()
                            WHEN NOT MATCHED THEN INSERT (
                              scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,
                              checksum,acquisition_time,bbox,crs,status,attributes
                            ) VALUES (
                              source.scene_id,source.dataset_id,source.scene_key,source.identity_key,
                              source.source_asset_id,source.source_uri,source.checksum,
                              source.acquisition_time,source.bbox,source.crs,'loaded',source.attributes
                            )
                            """,
                            (
                                scene["scene_id"],
                                dataset["dataset_id"],
                                scene["scene_key"],
                                scene["identity_key"],
                                scene["source_asset_id"],
                                scene["source_uri"],
                                scene.get("checksum"),
                                scene.get("acquisition_time"),
                                json.dumps(scene.get("bbox")),
                                scene.get("crs"),
                                json.dumps(scene["attributes"]),
                            ),
                        )
                        cursor.execute(
                            "SELECT scene_id,dataset_id FROM scenes WHERE identity_key = %s FOR UPDATE",
                            (scene["identity_key"],),
                        )
                        persisted = cursor.fetchone()
                        if persisted is None:
                            raise RuntimeError("scene merge returned no identity row")
                        persisted_scene_id, persisted_dataset_id = str(persisted[0]), str(persisted[1])
                        if persisted_dataset_id != dataset["dataset_id"]:
                            raise ValueError(
                                f"scene identity already belongs to dataset {persisted_dataset_id}: {scene['scene_key']}"
                            )
                        for asset in scene["assets"]:
                            cursor.execute(
                                """
                                MERGE INTO scene_assets target USING (
                                  SELECT %s::text AS scene_id, %s::text AS asset_id,
                                         %s::text AS source_uri, %s::text AS cog_uri,
                                         %s::text AS asset_role, %s::text AS source_kind,
                                         %s::text AS source_format, %s::text AS checksum,
                                         %s::timestamptz AS acquisition_time, %s::jsonb AS bbox,
                                         %s::text AS crs, %s::jsonb AS attributes
                                ) source ON (target.scene_id = source.scene_id AND target.asset_id = source.asset_id)
                                WHEN MATCHED THEN UPDATE SET
                                  source_uri = source.source_uri, cog_uri = source.cog_uri,
                                  asset_role = source.asset_role, source_kind = source.source_kind,
                                  source_format = source.source_format, checksum = source.checksum,
                                  acquisition_time = source.acquisition_time, bbox = source.bbox,
                                  crs = source.crs, attributes = target.attributes || source.attributes,
                                  updated_at = now()
                                WHEN NOT MATCHED THEN INSERT (
                                  scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,
                                  source_format,checksum,acquisition_time,bbox,crs,attributes
                                ) VALUES (
                                  source.scene_id,source.asset_id,source.source_uri,source.cog_uri,
                                  source.asset_role,source.source_kind,source.source_format,source.checksum,
                                  source.acquisition_time,source.bbox,source.crs,source.attributes
                                )
                                """,
                                (
                                    persisted_scene_id, asset["asset_id"], asset["source_uri"], asset.get("cog_uri"),
                                    asset["asset_role"], asset["source_kind"], asset["source_format"], asset.get("checksum"),
                                    asset.get("acquisition_time"), json.dumps(asset.get("bbox")), asset.get("crs"),
                                    json.dumps(asset["attributes"]),
                                ),
                            )
                            for band in asset["bands"]:
                                cursor.execute(
                                    """
                                    MERGE INTO scene_bands target USING (
                                      SELECT %s::text AS scene_id, %s::text AS asset_id,
                                             %s::text AS band_code, %s::text AS band_name,
                                             %s::text AS band_type, %s::text AS unit,
                                             %s::int AS display_order, %s::jsonb AS attributes
                                    ) source ON (
                                      target.scene_id = source.scene_id AND target.asset_id = source.asset_id
                                      AND target.band_code = source.band_code
                                    )
                                    WHEN MATCHED THEN UPDATE SET
                                      band_name = source.band_name, band_type = source.band_type,
                                      unit = source.unit, display_order = source.display_order,
                                      attributes = target.attributes || source.attributes
                                    WHEN NOT MATCHED THEN INSERT (
                                      scene_id,asset_id,band_code,band_name,band_type,unit,display_order,attributes
                                    ) VALUES (
                                      source.scene_id,source.asset_id,source.band_code,source.band_name,
                                      source.band_type,source.unit,source.display_order,source.attributes
                                    )
                                    """,
                                    (
                                        persisted_scene_id, asset["asset_id"], band["band_code"], band["band_name"],
                                        band["band_type"], band.get("unit"), band["display_order"], json.dumps(band["attributes"]),
                                    ),
                                )
                        cursor.execute(
                            """
                            MERGE INTO load_batch_scenes target USING (
                              SELECT %s::text AS load_batch_id, %s::text AS scene_id,
                                     %s::text AS source_asset_id, %s::text AS source_uri,
                                     %s::text AS checksum, %s::jsonb AS attributes
                            ) source ON (
                              target.load_batch_id = source.load_batch_id AND target.scene_id = source.scene_id
                            )
                            WHEN MATCHED THEN UPDATE SET
                              source_asset_id = source.source_asset_id, source_uri = source.source_uri,
                              checksum = source.checksum, load_status = 'succeeded', error_message = NULL,
                              attributes = target.attributes || source.attributes, updated_at = now()
                            WHEN NOT MATCHED THEN INSERT (
                              load_batch_id,scene_id,source_asset_id,source_uri,checksum,load_status,attributes
                            ) VALUES (
                              source.load_batch_id,source.scene_id,source.source_asset_id,
                              source.source_uri,source.checksum,'succeeded',source.attributes
                            )
                            """,
                            (
                                load_batch_id,
                                persisted_scene_id,
                                scene["source_asset_id"],
                                scene["source_uri"],
                                scene.get("checksum"),
                                json.dumps({"schema_import": True}),
                            ),
                        )
                        scene_count += 1
                cursor.execute(
                    "UPDATE load_batches SET status = 'succeeded', updated_at = now() WHERE load_batch_id = %s",
                    (load_batch_id,),
                )
            connection.commit()
        return {"load_batch_id": load_batch_id, "status": "succeeded", "dataset_count": len(datasets), "scene_count": scene_count}

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if self.connection_factory is not None:
            connection = self.connection_factory()
            if hasattr(connection, "__enter__"):
                with connection as borrowed:
                    yield borrowed
                return
            try:
                yield connection
            finally:
                close = getattr(connection, "close", None)
                if close is not None:
                    close()
            return
        if not self.dsn:
            raise RuntimeError("OpenGauss DSN is required")
        from cube_web.services.db_pool import _PostgresPool

        with _PostgresPool.for_dsn(self.dsn).connection() as connection:
            yield connection

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("lb.status = %s")
            params.append(status)
        if data_type:
            where.append(
                "EXISTS (SELECT 1 FROM load_batch_scenes typed_lbs "
                "JOIN scenes typed_scene ON typed_scene.scene_id = typed_lbs.scene_id "
                "JOIN datasets typed_dataset ON typed_dataset.dataset_id = typed_scene.dataset_id "
                "WHERE typed_lbs.load_batch_id = lb.load_batch_id AND typed_dataset.data_type = %s)"
            )
            params.append(data_type)
        if keyword:
            where.append("(lb.load_batch_id ILIKE %s OR lb.batch_name ILIKE %s)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        sql = """
            SELECT lb.*, count(DISTINCT s.dataset_id) AS dataset_count,
                   count(lbs.scene_id) AS scene_count
            FROM load_batches lb
            LEFT JOIN load_batch_scenes lbs ON lbs.load_batch_id = lb.load_batch_id
            LEFT JOIN scenes s ON s.scene_id = lbs.scene_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " GROUP BY lb.load_batch_id ORDER BY lb.created_at DESC LIMIT %s"
        params.append(limit)
        return self._read(sql, tuple(params))

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any] | None:
        rows = self._read(
            """
            SELECT lb.*, count(DISTINCT s.dataset_id) AS dataset_count,
                   count(lbs.scene_id) AS scene_count
            FROM load_batches lb
            LEFT JOIN load_batch_scenes lbs ON lbs.load_batch_id = lb.load_batch_id
            LEFT JOIN scenes s ON s.scene_id = lbs.scene_id
            WHERE lb.load_batch_id = %s
            GROUP BY lb.load_batch_id
            """,
            (load_batch_id,),
        )
        return rows[0] if rows else None

    def list_load_batch_scenes(
        self,
        load_batch_id: str,
        *,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where = ["lbs.load_batch_id = %s"]
        params: list[Any] = [load_batch_id]
        if status:
            where.append("lbs.load_status = %s")
            params.append(status)
        if data_type:
            where.append("d.data_type = %s")
            params.append(data_type)
        if dataset_id:
            where.append("d.dataset_id = %s")
            params.append(dataset_id)
        scenes = self._read(
            """
            SELECT s.*, d.dataset_code, d.dataset_title, d.data_type, d.product_type,
                   lbs.load_batch_id, lbs.load_status, lbs.error_message AS load_error,
                   lbs.attributes AS load_attributes
            FROM load_batch_scenes lbs
            JOIN scenes s ON s.scene_id = lbs.scene_id
            JOIN datasets d ON d.dataset_id = s.dataset_id
            WHERE """
            + " AND ".join(where)
            + " ORDER BY d.dataset_code, s.acquisition_time, s.scene_id",
            tuple(params),
        )
        scene_ids = [str(scene["scene_id"]) for scene in scenes]
        band_rows = self._read(
            "SELECT scene_id,asset_id,band_code,band_name,band_type,unit,display_order,attributes "
            "FROM scene_bands WHERE scene_id = ANY(%s::text[]) ORDER BY scene_id,display_order,band_code",
            (scene_ids,),
        ) if scene_ids else []
        bands_by_scene: dict[str, list[dict[str, Any]]] = {}
        for band in band_rows:
            bands_by_scene.setdefault(str(band["scene_id"]), []).append(band)
        for scene in scenes:
            scene["bands"] = bands_by_scene.get(str(scene["scene_id"]), [])
        return scenes

    def materialize_partition_datasets(self, request: ScenePartitionRunRequest) -> tuple[DatasetInput, ...]:
        selected_scene_ids = [scene_id for item in request.datasets for scene_id in item.scene_ids]
        batch_rows = self._read(
            "SELECT load_batch_id FROM load_batches WHERE load_batch_id = ANY(%s::text[]) ORDER BY load_batch_id",
            (list(request.source_batch_ids),),
        )
        known_batches = {str(row["load_batch_id"]) for row in batch_rows}
        missing_batches = sorted(set(request.source_batch_ids) - known_batches)
        if missing_batches:
            raise ValueError(f"source load batches not found: {missing_batches}")
        rows = self._read(
            """
            SELECT s.*, d.dataset_code, d.dataset_title, d.data_type, d.product_type,
                   d.attributes AS dataset_attributes, lbs.load_batch_id
            FROM scenes s
            JOIN datasets d ON d.dataset_id = s.dataset_id
            JOIN load_batch_scenes lbs ON lbs.scene_id = s.scene_id
            WHERE s.scene_id = ANY(%s::text[])
              AND lbs.load_batch_id = ANY(%s::text[])
              AND lbs.load_status IN ('succeeded','duplicate')
            ORDER BY d.dataset_id, s.scene_id, lbs.load_batch_id
            """,
            (selected_scene_ids, list(request.source_batch_ids)),
        )
        by_scene: dict[str, dict[str, Any]] = {}
        batches_by_scene: dict[str, set[str]] = {}
        for row in rows:
            scene_id = str(row["scene_id"])
            by_scene.setdefault(scene_id, row)
            batches_by_scene.setdefault(scene_id, set()).add(str(row["load_batch_id"]))
        missing = sorted(set(selected_scene_ids) - set(by_scene))
        if missing:
            raise ValueError(f"scenes are not eligible in source_batch_ids: {missing}")
        used_batches = {batch_id for values in batches_by_scene.values() for batch_id in values}
        unused_batches = sorted(set(request.source_batch_ids) - used_batches)
        if unused_batches:
            raise ValueError(f"source load batches contain no selected eligible scenes: {unused_batches}")
        asset_rows = self._read(
            "SELECT * FROM scene_assets WHERE scene_id = ANY(%s::text[]) AND asset_role = 'data' ORDER BY scene_id, asset_id",
            (selected_scene_ids,),
        )
        band_rows = self._read(
            "SELECT * FROM scene_bands WHERE scene_id = ANY(%s::text[]) ORDER BY scene_id, asset_id, display_order, band_code",
            (selected_scene_ids,),
        )
        assets_by_scene: dict[str, list[dict[str, Any]]] = {}
        bands_by_scene: dict[str, list[dict[str, Any]]] = {}
        for asset in asset_rows:
            assets_by_scene.setdefault(str(asset["scene_id"]), []).append(asset)
        for band in band_rows:
            bands_by_scene.setdefault(str(band["scene_id"]), []).append(band)

        materialized: list[DatasetInput] = []
        for selection in request.datasets:
            scene_rows = [by_scene[scene_id] for scene_id in selection.scene_ids]
            mismatched = [row["scene_id"] for row in scene_rows if str(row["dataset_id"]) != selection.dataset_id]
            if mismatched:
                raise ValueError(f"scenes do not belong to dataset {selection.dataset_id}: {mismatched}")
            assets: list[SourceAssetInput] = []
            bands: list[BandInput] = []
            for row in scene_rows:
                scene_id = str(row["scene_id"])
                scene_assets, scene_bands = _scene_inputs(
                    row,
                    assets_by_scene.get(scene_id, []),
                    bands_by_scene.get(scene_id, []),
                    batches_by_scene[scene_id],
                )
                assets.extend(scene_assets)
                bands.extend(scene_bands)
            first = scene_rows[0]
            materialized.append(
                DatasetInput(
                    dataset_id=selection.dataset_id,
                    dataset_code=str(first["dataset_code"]),
                    dataset_title=str(first["dataset_title"]),
                    data_type=str(first["data_type"]),
                    product_type=first.get("product_type"),
                    assets=tuple(assets),
                    bands=tuple(bands),
                    attributes=_json_object(first.get("dataset_attributes")),
                    partition=selection.partition,
                )
            )
        return tuple(materialized)

    def create_partition_run(self, request: ScenePartitionRunRequest) -> dict[str, Any]:
        fingerprint = sha256(request.model_dump_json().encode("utf-8")).hexdigest()
        claim_token = uuid4().hex
        claim_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    MERGE INTO partition_runs target USING (
                      SELECT %s::text AS partition_run_id, %s::jsonb AS source_load_batch_ids,
                             %s::jsonb AS attributes
                    ) source ON (target.partition_run_id = source.partition_run_id)
                    WHEN NOT MATCHED THEN INSERT (
                      partition_run_id,status,source_load_batch_ids,attributes
                    ) VALUES (
                      source.partition_run_id,'pending',source.source_load_batch_ids,source.attributes
                    )
                    """,
                    (
                        request.partition_run_id,
                        json.dumps(list(request.source_batch_ids)),
                        json.dumps({
                            "contract": "scene-domain-v1",
                            "request_fingerprint": fingerprint,
                            "claim_token": claim_token,
                            "claim_expires_at": claim_expires_at,
                        }),
                    ),
                )
                cursor.execute("SELECT * FROM partition_runs WHERE partition_run_id = %s FOR UPDATE", (request.partition_run_id,))
                run = _one(cursor)
                if run is None:
                    raise RuntimeError("partition run insert returned no row")
                attributes = _json_object(run.get("attributes"))
                if attributes.get("request_fingerprint") != fingerprint:
                    raise ValueError(f"partition_run_id belongs to a different request: {request.partition_run_id}")
                created = attributes.get("claim_token") == claim_token
                if not created:
                    cursor.execute(
                        """
                        UPDATE partition_runs
                        SET status = 'pending', error_message = NULL,
                            completed_at = NULL, attributes = attributes || %s::jsonb
                        WHERE partition_run_id = %s AND (attributes ->> 'task_id') IS NULL
                          AND (
                            status = 'failed'
                            OR (
                              status = 'pending' AND COALESCE(
                                NULLIF(attributes ->> 'claim_expires_at','')::timestamptz,
                                created_at + interval '60 seconds'
                              ) < now()
                            )
                          )
                        RETURNING *
                        """,
                        (
                            json.dumps({"claim_token": claim_token, "claim_expires_at": claim_expires_at}),
                            request.partition_run_id,
                        ),
                    )
                    reclaimed = _one(cursor)
                    if reclaimed is not None:
                        run = reclaimed
                        created = True
                if not created:
                    connection.commit()
                    return {**run, "created": False}
                for selection in request.datasets:
                    grid_config = selection.partition.model_dump(mode="json", exclude_none=True)
                    for scene_id in selection.scene_ids:
                        cursor.execute(
                            """
                            SELECT load_batch_id FROM load_batch_scenes
                            WHERE scene_id = %s AND load_batch_id = ANY(%s::text[])
                            ORDER BY load_batch_id LIMIT 1
                            """,
                            (scene_id, list(request.source_batch_ids)),
                        )
                        source_row = cursor.fetchone()
                        if source_row is None:
                            raise ValueError(f"scene is not linked to a selected load batch: {scene_id}")
                        source_load_batch_id = str(source_row[0])
                        identity = _partition_scene_idempotency_key(request.partition_run_id, scene_id, grid_config)
                        cursor.execute(
                            """
                            MERGE INTO partition_run_scenes target USING (
                              SELECT %s::text AS partition_run_id, %s::text AS scene_id,
                                %s::text AS dataset_id, %s::text AS source_load_batch_id,
                                %s::jsonb AS grid_config, %s::text AS idempotency_key
                            ) source ON (
                              target.partition_run_id = source.partition_run_id
                              AND target.scene_id = source.scene_id
                            )
                            WHEN NOT MATCHED THEN INSERT (
                              partition_run_id, scene_id, dataset_id, source_load_batch_id,
                              status, grid_config, idempotency_key
                            ) VALUES (
                              source.partition_run_id, source.scene_id, source.dataset_id,
                              source.source_load_batch_id, 'pending', source.grid_config,
                              source.idempotency_key
                            )
                            """,
                            (
                                request.partition_run_id,
                                scene_id,
                                selection.dataset_id,
                                source_load_batch_id,
                                json.dumps(grid_config),
                                identity,
                            ),
                        )
            connection.commit()
        return {**run, "created": True}

    def bind_partition_task(self, partition_run_id: str, task_id: str) -> None:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE partition_runs
                    SET status = 'queued',
                        attributes = (attributes - 'claim_token') || %s::jsonb
                    WHERE partition_run_id = %s
                    """,
                    (json.dumps({"task_id": task_id}), partition_run_id),
                )
                cursor.execute(
                    """
                    UPDATE partition_run_scenes SET status = 'queued', updated_at = now()
                    WHERE partition_run_id = %s AND status = 'pending'
                    """,
                    (partition_run_id,),
                )
            connection.commit()

    def rebind_partition_task(self, source_task_id: str, task_id: str) -> str | None:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT partition_run_id FROM partition_runs "
                    "WHERE attributes ->> 'task_id' = %s FOR UPDATE",
                    (source_task_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.commit()
                    return None
                partition_run_id = str(row[0])
                cursor.execute(
                    """
                    UPDATE partition_runs SET status='queued', error_message=NULL,
                      completed_at=NULL,
                      attributes=attributes || %s::jsonb
                    WHERE partition_run_id=%s
                    """,
                    (json.dumps({"task_id": task_id, "retry_source_task_id": source_task_id}), partition_run_id),
                )
                cursor.execute(
                    """
                    UPDATE partition_run_scenes SET status='queued', output_version=NULL,
                      error_message=NULL, updated_at=now()
                    WHERE partition_run_id=%s AND status <> 'completed'
                    """,
                    (partition_run_id,),
                )
            connection.commit()
        return partition_run_id

    def fail_partition_run(self, partition_run_id: str, error_message: str) -> None:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE partition_runs SET status = 'failed', error_message = %s,
                      attributes = attributes - 'claim_token', completed_at = now()
                    WHERE partition_run_id = %s AND status IN ('pending','queued')
                    """,
                    (error_message, partition_run_id),
                )
                cursor.execute(
                    """
                    UPDATE partition_run_scenes SET status = 'failed', error_message = %s,
                      attempt_count = attempt_count + 1, updated_at = now()
                    WHERE partition_run_id = %s AND status IN ('pending','queued')
                    """,
                    (error_message, partition_run_id),
                )
            connection.commit()

    def update_partition_task(self, task_id: str, status: str, result: dict[str, Any] | None = None) -> str | None:
        run_status = _partition_run_status(status, result)
        outcomes = {
            str(item.get("dataset_id")): item
            for item in (result or {}).get("datasets", [])
            if isinstance(item, dict) and item.get("dataset_id")
        }
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT partition_run_id FROM partition_runs WHERE attributes ->> 'task_id' = %s FOR UPDATE",
                    (task_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.commit()
                    return
                partition_run_id = str(row[0])
                cursor.execute(
                    """
                    UPDATE partition_runs SET status = %s,
                      started_at = CASE WHEN %s = 'running' THEN COALESCE(started_at,now()) ELSE started_at END,
                      completed_at = CASE WHEN %s IN ('completed','partial_failure','failed','cancelled') THEN now() ELSE completed_at END,
                      error_message = %s
                    WHERE partition_run_id = %s
                      AND (
                        CASE status
                          WHEN 'pending' THEN 0
                          WHEN 'queued' THEN 1
                          WHEN 'running' THEN 2
                          ELSE 3
                        END < %s OR status = %s
                      )
                    """,
                    (
                        run_status,
                        run_status,
                        run_status,
                        _error_text(None if result is None else result.get("error")),
                        partition_run_id,
                        _partition_status_rank(run_status),
                        run_status,
                    ),
                )
                if cursor.rowcount == 0:
                    connection.commit()
                    return partition_run_id
                if outcomes:
                    for dataset_id, outcome in outcomes.items():
                        scene_status = _partition_scene_status(str(outcome.get("status") or "failed"))
                        error = outcome.get("error") if isinstance(outcome.get("error"), dict) else {}
                        scene_outcomes = [item for item in outcome.get("scenes", ()) if isinstance(item, dict) and item.get("scene_id")]
                        if scene_outcomes:
                            for scene_outcome in scene_outcomes:
                                item_error = scene_outcome.get("error") if isinstance(scene_outcome.get("error"), dict) else {}
                                item_status = _partition_scene_status(str(scene_outcome.get("status") or "failed"))
                                cursor.execute(
                                    """
                                    UPDATE partition_run_scenes SET status = %s,
                                      output_version = CASE WHEN %s = 'completed' THEN %s ELSE NULL END,
                                      error_message = %s, attempt_count = attempt_count + 1, updated_at = now()
                                    WHERE partition_run_id = %s AND dataset_id = %s AND scene_id = %s
                                    """,
                                    (
                                        item_status, item_status, outcome.get("output_version"), item_error.get("message"),
                                        partition_run_id, dataset_id, scene_outcome["scene_id"],
                                    ),
                                )
                        else:
                            cursor.execute(
                                """
                                UPDATE partition_run_scenes SET status = %s, output_version = %s,
                                  error_message = %s, attempt_count = attempt_count + 1, updated_at = now()
                                WHERE partition_run_id = %s AND dataset_id = %s
                                """,
                                (
                                    scene_status, outcome.get("output_version"), error.get("message"),
                                    partition_run_id, dataset_id,
                                ),
                            )
                        if str(outcome.get("status") or "") == "completed" and outcome.get("output_version"):
                            cursor.execute(
                                """
                                UPDATE datasets SET current_output_version = %s, updated_at = now()
                                WHERE dataset_id = %s
                                """,
                                (outcome["output_version"], dataset_id),
                            )
                else:
                    cursor.execute(
                        """
                        UPDATE partition_run_scenes SET status = %s,
                          error_message = %s,
                          attempt_count = attempt_count + %s,
                          updated_at = now()
                        WHERE partition_run_id = %s
                          AND status IN ('pending','queued','running')
                        """,
                        (
                            _partition_scene_status(run_status),
                            _error_text(None if result is None else result.get("error")),
                            1 if run_status in {"completed", "partial_failure", "failed", "cancelled"} else 0,
                            partition_run_id,
                        ),
                    )
            connection.commit()
        return partition_run_id

    def _read(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return _all(cursor)


def _scene_inputs(
    row: dict[str, Any],
    asset_rows: list[dict[str, Any]],
    band_rows: list[dict[str, Any]],
    source_batch_ids: set[str],
) -> tuple[list[SourceAssetInput], list[BandInput]]:
    if not asset_rows:
        raise ValueError(f"scene data assets are required for partition: {row['scene_id']}")
    scene_attributes = _json_object(row.get("attributes"))
    data_type = str(row["data_type"])
    assets: list[SourceAssetInput] = []
    for asset_row in asset_rows:
        attributes = _json_object(asset_row.get("attributes"))
        source_asset_id = str(asset_row["asset_id"])
        source_uri = str(asset_row.get("source_uri") or "")
        checksum = str(asset_row.get("checksum") or attributes.get("checksum") or "")
        if len(checksum) != 64:
            raise ValueError(f"scene asset checksum is required for partition: {row['scene_id']}/{source_asset_id}")
        acquisition = _iso(asset_row.get("acquisition_time") or row.get("acquisition_time") or attributes.get("time_start"))
        time_end = _iso(attributes.get("time_end") or asset_row.get("acquisition_time") or row.get("acquisition_time"))
        if not acquisition or not time_end:
            raise ValueError(f"scene asset acquisition time is required for partition: {row['scene_id']}/{source_asset_id}")
        asset_attributes = {
            **scene_attributes,
            **attributes,
            "scene_id": str(row["scene_id"]),
            "source_batch_ids": sorted(source_batch_ids),
        }
        if data_type == "carbon":
            source_format = str(asset_row.get("source_format") or _source_format(source_uri))
            assets.append(SourceAssetInput(
                source_asset_id=source_asset_id,
                source_uri=source_uri,
                source_kind="raw",
                source_format=source_format,
                checksum=checksum,
                bbox=asset_row.get("bbox") or row.get("bbox"),
                crs=asset_row.get("crs") or row.get("crs"),
                time_start=acquisition,
                time_end=time_end,
                attributes=asset_attributes,
            ))
        else:
            cog_uri = str(asset_row.get("cog_uri") or source_uri)
            assets.append(SourceAssetInput(
                source_asset_id=source_asset_id,
                cog_uri=cog_uri,
                source_kind="cog",
                source_format="cog",
                checksum=checksum,
                bbox=asset_row.get("bbox") or row.get("bbox"),
                crs=asset_row.get("crs") or row.get("crs"),
                time_start=acquisition,
                time_end=time_end,
                attributes=asset_attributes,
            ))
    bands = [
        BandInput(
            source_asset_id=str(item["asset_id"]),
            band_code=str(item["band_code"]),
            band_name=str(item.get("band_name") or item["band_code"]),
            band_type=str(item.get("band_type") or ("variable" if data_type in {"product", "carbon"} else "spectral")),
            unit=item.get("unit"),
            display_order=int(item.get("display_order") or 0),
            attributes=_json_object(item.get("attributes")),
        )
        for item in band_rows
    ]
    if not bands:
        raise ValueError(f"scene bands are required for partition: {row['scene_id']}")
    return assets, bands


def _load_schema_datasets(payload: dict[str, Any], *, load_batch_id: str) -> list[dict[str, Any]]:
    raw_datasets = payload.get("datasets")
    if not isinstance(raw_datasets, list) or not raw_datasets:
        raise ValueError("datasets must be a non-empty list")
    datasets = [_normalize_load_dataset(item, payload=payload, load_batch_id=load_batch_id) for item in raw_datasets]
    dataset_ids = [item["dataset_id"] for item in datasets]
    if len(set(dataset_ids)) != len(dataset_ids):
        raise ValueError("duplicate dataset_id in load schema")
    return datasets


def _normalize_load_dataset(item: Any, *, payload: dict[str, Any], load_batch_id: str) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError("datasets entries must be objects")
    dataset_id = str(item.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("datasets[].dataset_id is required")
    data_type = str(item.get("data_type") or "").strip().lower()
    if data_type not in {"optical", "radar", "product", "carbon"}:
        raise ValueError(f"unsupported dataset data_type: {data_type}")
    raw_scenes = item.get("scenes")
    if raw_scenes is None:
        raw_scenes = item.get("observations") if data_type == "carbon" else item.get("assets")
    scenes = [
        _normalize_load_scene(
            scene,
            data_type=data_type,
            product_type=item.get("product_type"),
            source_system=str(payload.get("source_system") or "loader"),
            index=index,
        )
        for index, scene in enumerate(raw_scenes or [])
    ]
    if not scenes:
        raise ValueError(f"dataset {dataset_id} requires at least one scene")
    scene_keys = [scene["scene_key"] for scene in scenes]
    if len(set(scene_keys)) != len(scene_keys):
        raise ValueError(f"duplicate scene_key in dataset {dataset_id}")
    return {
        "dataset_id": dataset_id,
        "dataset_code": str(item.get("dataset_code") or dataset_id),
        "dataset_title": str(item.get("dataset_title") or item.get("title") or dataset_id),
        "data_type": data_type,
        "product_type": item.get("product_type"),
        "status": str(item.get("status") or "active"),
        "assignment_confidence": float(item.get("assignment_confidence", 1)),
        "assignment_issue": item.get("assignment_issue"),
        "auto_ingest_allowed": bool(item.get("auto_ingest_allowed", True)),
        "attributes": {**_json_object(item.get("attributes")), "last_load_batch_id": load_batch_id},
        "scenes": scenes,
    }


def _normalize_load_scene(
    item: Any,
    *,
    data_type: str,
    product_type: Any | None,
    source_system: str,
    index: int,
) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError("scenes entries must be objects")
    raw_assets = item.get("assets")
    has_explicit_assets = isinstance(raw_assets, list) and bool(raw_assets)
    if isinstance(raw_assets, list) and raw_assets and item.get("bands") is not None:
        raise ValueError("multi-asset scenes must declare bands on each asset")
    if not isinstance(raw_assets, list) or not raw_assets:
        raw_assets = [item]
    assets = [
        _normalize_scene_asset(
            asset,
            data_type=data_type,
            product_type=product_type,
            index=asset_index,
        )
        for asset_index, asset in enumerate(raw_assets)
    ]
    primary = next((asset for asset in assets if asset["asset_role"] == "data"), assets[0])
    scene_key = str(
        item.get("scene_key")
        or item.get("observation_id")
        or (None if has_explicit_assets else item.get("asset_id") or item.get("source_asset_id") or primary["asset_id"])
        or item.get("scene_id")
        or item.get("asset_id")
        or primary["source_uri"]
    )
    provider_namespace = str(
        item.get("source_namespace") or item.get("provider") or item.get("sensor") or source_system
    ).strip().lower()
    identity_key = sha256(f"{provider_namespace}\0{scene_key}".encode("utf-8")).hexdigest()
    scene_id = str(item.get("canonical_scene_id") or f"scene-{identity_key}")
    attributes = _json_object(item.get("attributes"))
    for key in (
        "bands", "band", "variable", "source_format", "time_end",
        "resolution", "resolution_m", "spatial_resolution", "spatial_resolution_m",
        "ground_resolution", "pixel_size", "pixel_size_m", "gsd", "gsd_m",
    ):
        if item.get(key) is not None:
            attributes[key] = item[key]
    primary_attributes = primary.get("attributes") if isinstance(primary.get("attributes"), dict) else {}
    if not any(key in attributes for key in ("resolution", "resolution_m", "spatial_resolution", "spatial_resolution_m", "ground_resolution", "pixel_size", "pixel_size_m", "gsd", "gsd_m")):
        for key in ("resolution", "resolution_m", "spatial_resolution", "spatial_resolution_m", "ground_resolution", "pixel_size", "pixel_size_m", "gsd", "gsd_m"):
            if primary_attributes.get(key) is not None:
                attributes[key] = primary_attributes[key]
    if "bands" not in attributes and item.get("band") is not None:
        attributes["bands"] = [item["band"]]
    if "bands" not in attributes and item.get("variable") is not None:
        attributes["bands"] = [
            {"band_code": str(item["variable"]), "band_name": str(item["variable"]), "band_type": "variable", "unit": item.get("unit")}
        ]
    return {
        "scene_id": scene_id,
        "scene_key": scene_key,
        "identity_key": identity_key,
        "source_asset_id": primary["asset_id"],
        "source_uri": primary["source_uri"],
        "checksum": primary.get("checksum"),
        "acquisition_time": primary.get("acquisition_time"),
        "bbox": primary.get("bbox"),
        "crs": primary.get("crs"),
        "attributes": {**attributes, "data_type": data_type},
        "assets": assets,
    }


def _normalize_scene_asset(
    item: Any,
    *,
    data_type: str,
    product_type: Any | None,
    index: int,
) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError("scene assets entries must be objects")
    source_uri = str(item.get("source_uri") or item.get("cog_uri") or "").strip()
    if not source_uri.startswith("s3://"):
        raise ValueError("scene asset source_uri must use s3://")
    asset_id = str(
        item.get("asset_id")
        or item.get("source_asset_id")
        or item.get("observation_id")
        or f"asset-{sha256(source_uri.encode('utf-8')).hexdigest()[:24]}"
    )
    source_kind = str(item.get("source_kind") or ("observation" if data_type == "carbon" else "cog"))
    source_format = str(item.get("source_format") or (_source_format(source_uri) if data_type == "carbon" else "cog"))
    raw_bands = item.get("bands")
    if not isinstance(raw_bands, list):
        raw_bands = []
    if not raw_bands and item.get("band") is not None:
        raw_bands = [item["band"]]
    if not raw_bands and item.get("variable") is not None:
        raw_bands = [{"band_code": item["variable"], "band_name": item["variable"], "band_type": "variable", "unit": item.get("unit")}]
    bands = []
    for band_index, raw_band in enumerate(raw_bands):
        band = raw_band if isinstance(raw_band, dict) else {"band_code": str(raw_band), "band_name": str(raw_band)}
        band_code = str(band.get("band_code") or band.get("code") or band.get("band") or "").strip()
        generated_code = not band_code
        if not band_code:
            basis = f"{data_type}\0{str(product_type or '').strip().lower()}\0{asset_id}\0{band_index}"
            band_code = f"auto-{data_type}-{sha256(basis.encode('utf-8')).hexdigest()[:12]}-{band_index + 1}"
        band_type = str(band.get("band_type") or _default_band_type(data_type)).strip()
        if band_type not in {"spectral", "polarization", "variable"}:
            raise ValueError(f"unsupported band_type: {band_type}")
        band_attributes = _json_object(band.get("attributes"))
        if generated_code:
            band_attributes = {
                **band_attributes,
                "band_code_generated": True,
                "band_code_basis": "data_type+product_type+asset_id+band_index",
            }
        bands.append({
            "band_code": band_code,
            "band_name": str(band.get("band_name") or band.get("name") or band_code),
            "band_type": band_type,
            "unit": band.get("unit"),
            "display_order": int(band.get("display_order", band_index)),
            "attributes": band_attributes,
        })
    attributes = _json_object(item.get("attributes"))
    for key in (
        "resolution", "resolution_m", "spatial_resolution", "spatial_resolution_m",
        "ground_resolution", "pixel_size", "pixel_size_m", "gsd", "gsd_m",
    ):
        if item.get(key) is not None:
            attributes[key] = item[key]
    return {
        "asset_id": asset_id,
        "source_uri": source_uri,
        "cog_uri": item.get("cog_uri") or (source_uri if source_kind == "cog" else None),
        "asset_role": str(item.get("asset_role") or "data"),
        "source_kind": source_kind,
        "source_format": source_format,
        "checksum": item.get("checksum"),
        "acquisition_time": item.get("acquisition_time") or item.get("acq_time") or item.get("time_start"),
        "bbox": item.get("bbox"),
        "crs": item.get("crs"),
        "attributes": attributes,
        "bands": bands,
    }


def _default_band_type(data_type: str) -> str:
    if data_type in {"product", "carbon"}:
        return "variable"
    if data_type == "radar":
        return "polarization"
    return "spectral"


def _partition_scene_idempotency_key(partition_run_id: str, scene_id: str, grid_config: dict[str, Any]) -> str:
    canonical = json.dumps(grid_config, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha256(f"{partition_run_id}\0{scene_id}\0{canonical}".encode("utf-8")).hexdigest()


def _partition_run_status(task_status: str, result: dict[str, Any] | None) -> str:
    candidate = str((result or {}).get("status") or task_status or "failed")
    aliases = {"succeeded": "completed", "cancel_requested": "running"}
    candidate = aliases.get(candidate, candidate)
    if candidate not in {"pending", "queued", "running", "completed", "partial_failure", "failed", "cancelled"}:
        return "failed"
    return candidate


def _partition_status_rank(status: str) -> int:
    if status == "pending":
        return 0
    if status == "queued":
        return 1
    if status == "running":
        return 2
    return 3


def _partition_scene_status(status: str) -> str:
    aliases = {"succeeded": "completed", "partial_failure": "failed", "cancel_requested": "running"}
    candidate = aliases.get(status, status)
    if candidate not in {"pending", "queued", "running", "completed", "failed", "cancelled"}:
        return "failed"
    return candidate


def _error_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return str(value.get("message") or value.get("code") or "partition task failed")
    return str(value)


def _source_format(uri: str) -> str:
    lowered = uri.lower()
    if lowered.endswith((".h5", ".hdf", ".hdf5")):
        return "hdf5"
    return "netcdf"


def _iso(value: Any) -> str:
    if value in {None, ""}:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("expected JSON object")


def _all(cursor: Any) -> list[dict[str, Any]]:
    columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _one(cursor: Any) -> dict[str, Any] | None:
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or ()]
    return dict(zip(columns, row))
