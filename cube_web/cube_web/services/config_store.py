from __future__ import annotations

import copy
import json
from datetime import datetime
from typing import Any

from cube_split import runtime_config
from grid_core.app.core.enums import GridType
from grid_core.app.models.request import validate_requested_grid_level

CONFIG_SCOPE = "cube_web"
DEFAULT_CONFIG: dict[str, Any] = {
    "partition": {
        "optical": {
            "grid_type": "geohash",
            "grid_level": 5,
            "cover_mode": "intersect",
            "time_granularity": "day",
            "max_cells_per_asset": 0,
            "partition_backend": "ray",
            "ray_parallelism": 0,
            "partition_prefix_len": 3,
            "chunk_size": 0,
            "product_family": "auto",
            "sample_mean": False,
        }
    },
    "ingest": {
        "optical": {
            "dataset": "demo_optical",
            "sensor": "optical_mosaic",
            "quality_rule": "best_quality_wins",
            "allow_failed_quality": False,
            "metadata_backend": "postgres",
            "asset_storage_backend": "minio",
            "minio_endpoint": "",
            "minio_bucket": runtime_config.DEFAULT_MINIO_BUCKET,
            "minio_prefix": "cube/entity",
            "minio_secure": False,
            "minio_upload_workers": 8,
            "postgres_batch_size": 1000,
        }
    },
    "quality": {
        "optical": {
            "target_crs": "EPSG:4326",
            "history_limit": 20,
        }
    },
}


class ConfigStore:
    def ensure_schema(self) -> None:
        raise NotImplementedError

    def get_config_record(self) -> dict[str, Any]:
        raise NotImplementedError

    def update_config(self, config: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def reset_config(self) -> dict[str, Any]:
        raise NotImplementedError


class PostgresConfigStore(ConfigStore):
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("PostgreSQL DSN is required")
        self.dsn = dsn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cube_web_configs (
                      scope TEXT PRIMARY KEY,
                      config JSONB NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_cube_web_configs_config_gin
                    ON cube_web_configs USING GIN (config)
                    """
                )
            conn.commit()

    def get_config_record(self) -> dict[str, Any]:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config, updated_at FROM cube_web_configs WHERE scope = %s", (CONFIG_SCOPE,))
                row = cur.fetchone()
        if row is None:
            config = default_config()
            return {"config": config, "updated_at": None}
        return {"config": normalized_stored_config(row[0]), "updated_at": _iso_datetime(row[1])}

    def update_config(self, config: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        normalized = normalized_config(config)
        stored = stored_config(normalized)
        params = {"scope": CONFIG_SCOPE, "config": self._jsonb(stored)}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO cube_web_configs target
                    USING (SELECT %(scope)s::text AS scope, %(config)s::jsonb AS config) source
                    ON (target.scope = source.scope)
                    WHEN MATCHED THEN UPDATE SET
                      config = source.config,
                      updated_at = now()
                    WHEN NOT MATCHED THEN INSERT (scope, config)
                      VALUES (source.scope, source.config)
                    """,
                    params,
                )
                cur.execute("SELECT config, updated_at FROM cube_web_configs WHERE scope = %s", (CONFIG_SCOPE,))
                row = cur.fetchone()
            conn.commit()
        return {"config": normalized_stored_config(row[0]), "updated_at": _iso_datetime(row[1])}

    def reset_config(self) -> dict[str, Any]:
        return self.update_config(default_config())

    def _connect(self):
        try:
            from cube_web.services.db_pool import _PostgresPool
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("PostgreSQL config storage requires `psycopg`") from exc
        return _PostgresPool.for_dsn(self.dsn).connection()

    def _jsonb(self, value: dict[str, Any]):
        from psycopg.types.json import Jsonb

        return Jsonb(value, dumps=lambda item: json.dumps(item, ensure_ascii=False))


_store: ConfigStore | None = None


def get_config_store() -> ConfigStore:
    global _store
    if _store is None:
        _store = PostgresConfigStore(runtime_config.require_postgres_dsn())
    return _store


def set_config_store(store: ConfigStore | None) -> None:
    global _store
    _store = store


def get_app_config() -> dict[str, Any]:
    return get_config_store().get_config_record()["config"]


def default_config() -> dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    minio = runtime_config.minio_settings()
    config["ingest"]["optical"]["minio_endpoint"] = minio.endpoint
    config["ingest"]["optical"]["minio_bucket"] = minio.bucket
    config["ingest"]["optical"]["minio_secure"] = minio.secure
    return config


def normalized_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if config is not None and not isinstance(config, dict):
        raise ValueError("config must be an object")
    merged = _deep_merge(default_config(), config or {})
    optical = merged["partition"]["optical"]
    optical["grid_type"] = _choice(optical.get("grid_type"), {"geohash", "mgrs", "isea4h"}, "grid_type")
    optical["grid_level"] = _int_value(optical.get("grid_level"), "grid_level", minimum=0)
    validate_requested_grid_level(GridType(optical["grid_type"]), optical["grid_level"])
    optical["cover_mode"] = _choice(optical.get("cover_mode"), {"intersect", "contain", "minimal"}, "cover_mode")
    optical["time_granularity"] = _choice(optical.get("time_granularity"), {"second", "minute", "hour", "day", "month"}, "time_granularity")
    optical["max_cells_per_asset"] = _int_value(optical.get("max_cells_per_asset"), "max_cells_per_asset", minimum=0)
    optical["partition_backend"] = _choice(optical.get("partition_backend"), {"ray", "thread", "process"}, "partition_backend")
    optical["ray_parallelism"] = _int_value(optical.get("ray_parallelism"), "ray_parallelism", minimum=0)
    optical["partition_prefix_len"] = _int_value(optical.get("partition_prefix_len"), "partition_prefix_len", minimum=1)
    optical["chunk_size"] = _int_value(optical.get("chunk_size"), "chunk_size", minimum=0)
    optical["product_family"] = _text_value(optical.get("product_family"), "product_family")
    optical["sample_mean"] = bool(optical.get("sample_mean", False))

    ingest = merged["ingest"]["optical"]
    ingest["dataset"] = _text_value(ingest.get("dataset"), "dataset")
    ingest["sensor"] = _text_value(ingest.get("sensor"), "sensor")
    if "asset_version" in ingest:
        ingest["asset_version"] = _text_value(ingest.get("asset_version"), "asset_version")
    ingest["quality_rule"] = _choice(ingest.get("quality_rule"), {"best_quality_wins", "latest_wins"}, "quality_rule")
    ingest["allow_failed_quality"] = bool(ingest.get("allow_failed_quality", False))
    ingest["metadata_backend"] = _choice(ingest.get("metadata_backend"), {"none", "local", "postgres"}, "metadata_backend")
    ingest["asset_storage_backend"] = _choice(ingest.get("asset_storage_backend"), {"local", "minio"}, "asset_storage_backend")
    ingest["minio_endpoint"] = str(ingest.get("minio_endpoint") or "").strip()
    ingest["minio_bucket"] = str(ingest.get("minio_bucket") or "").strip()
    ingest["minio_prefix"] = _text_value(ingest.get("minio_prefix"), "minio_prefix")
    ingest["minio_secure"] = bool(ingest.get("minio_secure", False))
    ingest["minio_upload_workers"] = _int_value(ingest.get("minio_upload_workers"), "minio_upload_workers", minimum=1)
    ingest["postgres_batch_size"] = _int_value(ingest.get("postgres_batch_size"), "postgres_batch_size", minimum=1)

    quality = merged["quality"]["optical"]
    quality["target_crs"] = _text_value(quality.get("target_crs"), "quality.target_crs")
    quality["history_limit"] = _int_value(quality.get("history_limit"), "history_limit", minimum=1, maximum=200)
    return merged


def normalized_stored_config(config: dict[str, Any] | None) -> dict[str, Any]:
    return normalized_config(copy.deepcopy(config or {}))


def stored_config(config: dict[str, Any] | None) -> dict[str, Any]:
    stored = copy.deepcopy(config or {})
    optical = stored.get("ingest", {}).get("optical", {})
    if isinstance(optical, dict):
        optical.pop("minio_endpoint", None)
        optical.pop("minio_bucket", None)
        optical.pop("minio_secure", None)
    return stored


def optical_partition_defaults() -> dict[str, Any]:
    config = get_app_config()
    optical = config.get("partition", {}).get("optical", {})
    return dict(optical) if isinstance(optical, dict) else dict(DEFAULT_CONFIG["partition"]["optical"])


def optical_ingest_defaults() -> dict[str, Any]:
    config = get_app_config()
    optical = config.get("ingest", {}).get("optical", {})
    return dict(optical) if isinstance(optical, dict) else dict(DEFAULT_CONFIG["ingest"]["optical"])


def optical_quality_defaults() -> dict[str, Any]:
    config = get_app_config()
    optical = config.get("quality", {}).get("optical", {})
    return dict(optical) if isinstance(optical, dict) else dict(DEFAULT_CONFIG["quality"]["optical"])


def runtime_info() -> dict[str, Any]:
    postgres = _postgres_dsn()
    minio = runtime_config.minio_settings()
    return {
        "postgres_dsn": _masked_dsn(postgres) if postgres else "",
        "ray_address": runtime_config.ray_address(),
        "minio": {
            "endpoint": minio.endpoint,
            "bucket": minio.bucket,
            "secure": minio.secure,
        },
        "config_scope": CONFIG_SCOPE,
    }


def _postgres_dsn() -> str:
    return runtime_config.postgres_dsn()


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for key, value in override.items():
        if key not in base:
            continue
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(dict(base[key]), value)
        else:
            base[key] = value
    return base


def _choice(value: Any, allowed: set[str], name: str) -> str:
    text = _text_value(value, name)
    if text not in allowed:
        raise ValueError(f"{name} must be one of: {', '.join(sorted(allowed))}")
    return text


def _text_value(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    return text


def _int_value(value: Any, name: str, *, minimum: int, maximum: int | None = None) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer") from None
    if number < minimum:
        raise ValueError(f"{name} must be greater than or equal to {minimum}")
    if maximum is not None and number > maximum:
        raise ValueError(f"{name} must be less than or equal to {maximum}")
    return number


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _masked_dsn(dsn: str) -> str:
    if "@" not in dsn or "://" not in dsn:
        return dsn
    scheme, rest = dsn.split("://", 1)
    if "@" not in rest:
        return dsn
    _, host = rest.rsplit("@", 1)
    return f"{scheme}://***:***@{host}"
