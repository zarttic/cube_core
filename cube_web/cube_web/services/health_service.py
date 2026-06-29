from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Iterable

from cube_split import runtime_config

from cube_web.services.config_store import _masked_dsn

ACTIVE_CHECKS = ("postgres", "ray", "minio", "bucket")
ALL_CHECKS = ("config", *ACTIVE_CHECKS)


def health_report(requested_checks: Iterable[str] | None = None) -> dict[str, Any]:
    checks = {"service": {"status": "ok"}}
    selected = _selected_checks(requested_checks)
    for name in selected:
        if name == "config":
            checks[name] = _check_config_sources()
        elif name == "postgres":
            checks[name] = _check_postgres()
        elif name == "ray":
            checks[name] = _check_ray()
        elif name == "minio":
            checks[name] = _check_minio()
        elif name == "bucket":
            checks[name] = _check_minio_bucket()

    failed = [name for name, item in checks.items() if item.get("status") == "fail"]
    return {
        "status": "degraded" if failed else "ok",
        "checks": checks,
        "failed_checks": failed,
    }


def _selected_checks(requested_checks: Iterable[str] | None) -> list[str]:
    names: list[str] = []
    for raw in requested_checks or ():
        for item in str(raw or "").split(","):
            name = item.strip().lower()
            if not name:
                continue
            if name in {"all", "deep", "full"}:
                names.extend(ALL_CHECKS)
            elif name in {"postgresql", "db", "database"}:
                names.append("postgres")
            elif name in ALL_CHECKS:
                names.append(name)
    if not names:
        names = ["config"]
    result: list[str] = []
    for name in names:
        if name not in result:
            result.append(name)
    return result


def _check_config_sources() -> dict[str, Any]:
    minio = runtime_config.minio_settings()
    postgres_value, postgres_source = _setting_source(
        ("CUBE_WEB_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL"),
        default=runtime_config.DEFAULT_POSTGRES_DSN,
    )
    ray_value, ray_source = _setting_source(
        ("CUBE_WEB_RAY_ADDRESS", "RAY_ADDRESS"),
        default=runtime_config.DEFAULT_RAY_ADDRESS,
    )
    minio_endpoint, minio_endpoint_source = _setting_source(
        ("CUBE_WEB_MINIO_ENDPOINT", "MINIO_ENDPOINT"),
        default=runtime_config.DEFAULT_MINIO_ENDPOINT,
    )
    minio_access_key, minio_access_key_source = _setting_source(
        ("CUBE_WEB_MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY"),
        default=runtime_config.DEFAULT_MINIO_ACCESS_KEY,
        service_env_key="MINIO_ROOT_USER",
    )
    minio_secret_key, minio_secret_key_source = _setting_source(
        ("CUBE_WEB_MINIO_SECRET_KEY", "MINIO_SECRET_KEY"),
        default=runtime_config.DEFAULT_MINIO_SECRET_KEY,
        service_env_key="MINIO_ROOT_PASSWORD",
    )
    minio_bucket, minio_bucket_source = _setting_source(
        ("CUBE_WEB_MINIO_BUCKET", "MINIO_BUCKET"),
        default=runtime_config.DEFAULT_MINIO_BUCKET,
    )
    return {
        "status": "ok",
        "values": {
            "postgres_dsn": _config_value(bool(postgres_value), postgres_source, value=_masked_dsn(postgres_value) if postgres_value else ""),
            "ray_address": _config_value(bool(ray_value), ray_source, value=ray_value),
            "minio_endpoint": _config_value(bool(minio_endpoint), minio_endpoint_source, value=minio.endpoint),
            "minio_access_key": _config_value(bool(minio_access_key), minio_access_key_source),
            "minio_secret_key": _config_value(bool(minio_secret_key), minio_secret_key_source),
            "minio_bucket": _config_value(bool(minio_bucket), minio_bucket_source, value=minio.bucket),
            "minio_secure": {
                "configured": True,
                "source": "runtime_default",
                "value": minio.secure,
            },
        },
    }


def _check_postgres() -> dict[str, Any]:
    dsn = runtime_config.postgres_dsn()
    if not dsn:
        return _fail("PostgreSQL DSN is not configured")
    start = time.perf_counter()
    try:
        import psycopg

        from cube_web.services.db_pool import _PostgresPool

        with _PostgresPool.for_dsn(dsn).connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:
        return _fail(str(exc), error_type=exc.__class__.__name__)
    return {
        "status": "ok",
        "latency_ms": _elapsed_ms(start),
        "dsn": _masked_dsn(dsn),
    }


def _check_ray() -> dict[str, Any]:
    address = runtime_config.ray_address()
    if not address:
        return _fail("Ray address is not configured")
    start = time.perf_counter()
    ray = None
    already_initialized = False
    try:
        import ray  # type: ignore

        already_initialized = ray.is_initialized()
        if not already_initialized:
            ray.init(address=address, ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")
        resources = ray.cluster_resources()
        nodes = ray.nodes()
    except Exception as exc:
        return _fail(str(exc), error_type=exc.__class__.__name__)
    finally:
        if ray is not None and not already_initialized:
            try:
                ray.shutdown()
            except Exception:
                pass
    return {
        "status": "ok",
        "latency_ms": _elapsed_ms(start),
        "address": address,
        "alive_nodes": sum(1 for node in nodes if node.get("Alive")),
        "resources": resources,
    }


def _check_minio() -> dict[str, Any]:
    start = time.perf_counter()
    try:
        client, settings = _minio_client()
        buckets = client.list_buckets()
    except Exception as exc:
        return _fail(str(exc), error_type=exc.__class__.__name__)
    return {
        "status": "ok",
        "latency_ms": _elapsed_ms(start),
        "endpoint": settings.endpoint,
        "secure": settings.secure,
        "bucket_count": len(buckets),
    }


def _check_minio_bucket() -> dict[str, Any]:
    start = time.perf_counter()
    try:
        client, settings = _minio_client()
        exists = client.bucket_exists(settings.bucket)
    except Exception as exc:
        return _fail(str(exc), error_type=exc.__class__.__name__)
    if not exists:
        return _fail(f"MinIO bucket does not exist: {settings.bucket}", bucket=settings.bucket)
    return {
        "status": "ok",
        "latency_ms": _elapsed_ms(start),
        "bucket": settings.bucket,
    }


def _minio_client():
    from minio import Minio

    settings = runtime_config.minio_settings()
    missing = [
        name
        for name, value in (
            ("endpoint", settings.endpoint),
            ("access_key", settings.access_key),
            ("secret_key", settings.secret_key),
            ("bucket", settings.bucket),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"MinIO settings are incomplete: {', '.join(missing)}")
    return (
        Minio(
            settings.endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=settings.secure,
        ),
        settings,
    )


def _setting_source(
    names: tuple[str, ...],
    *,
    default: str,
    service_env_key: str | None = None,
) -> tuple[str, dict[str, str]]:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip(), {"source": "environment", "name": name}

    env_values, env_path = _local_env_values_with_path()
    for name in names:
        value = env_values.get(name)
        if value is not None and value.strip():
            source = {"source": "env_file", "name": name}
            if env_path is not None:
                source["path"] = str(env_path)
            return value.strip(), source

    if service_env_key:
        service_value = runtime_config.minio_service_env().get(service_env_key)
        if service_value:
            return service_value, {"source": "minio_service_env", "name": service_env_key}

    if default:
        return default, {"source": "default"}
    return "", {"source": "missing"}


def _local_env_values_with_path() -> tuple[dict[str, str], Path | None]:
    env_file = os.environ.get("CUBE_WEB_ENV_FILE")
    if env_file:
        path = Path(env_file)
        return runtime_config.read_env_file(path), path
    for path in runtime_config.env_file_candidates():
        values = runtime_config.read_env_file(path)
        if values:
            return values, path
    return {}, None


def _config_value(configured: bool, source: dict[str, str], *, value: Any | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"configured": configured, **source}
    if value is not None:
        item["value"] = value
    return item


def _fail(message: str, **details: Any) -> dict[str, Any]:
    result = {"status": "fail", "message": message}
    result.update(details)
    return result


def _elapsed_ms(start: float) -> int:
    return int(round((time.perf_counter() - start) * 1000))
