from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

DEFAULT_POSTGRES_DSN = ""
DEFAULT_RAY_ADDRESS = ""
DEFAULT_MINIO_ENDPOINT = ""
DEFAULT_MINIO_ACCESS_KEY = ""
DEFAULT_MINIO_SECRET_KEY = ""
DEFAULT_MINIO_BUCKET = "cube"
DEFAULT_AUTH_MAIN_SYSTEM_URL = ""
DEFAULT_AUTH_CLIENT_ID = "system_ard"
DEFAULT_AUTH_REDIRECT_URI = "/callback"
DEFAULT_AUTH_TOKEN_PATH = "/api/token"
DEFAULT_AUTH_LOGOUT_PATH = "/api/logout"
DEFAULT_PORTAL_DATA_PATH = "/ard"
DEFAULT_PORTAL_ADMIN_PATH = "/admin"
DEFAULT_PORTAL_HOME_URL = "http://10.136.1.14:5176/#/home"
DEFAULT_PORTAL_DATA_INGEST_URL = "http://10.136.1.14:5177/ard"
DEFAULT_PORTAL_PARTITION_SERVICE_URL = "http://10.136.1.14:5176/#/partition"
DEFAULT_PORTAL_DISPATCH_URL = "http://10.136.1.14:5176/#/dispatch"
DEFAULT_PORTAL_ADMIN_URL = "http://10.136.1.14:5177/admin"


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False


@dataclass(frozen=True)
class AuthSettings:
    main_system_url: str
    client_id: str
    client_secret: str
    redirect_uri: str
    jwt_secret_key: str
    jwt_algorithm: str
    token_path: str
    logout_path: str
    required: bool


@dataclass(frozen=True)
class PortalSettings:
    main_system_url: str
    home_url: str
    data_ingest_url: str
    partition_service_url: str
    dispatch_url: str
    admin_url: str


def postgres_dsn() -> str:
    return env_text("CUBE_WEB_POSTGRES_DSN") or env_text("POSTGRES_DSN") or env_text("DATABASE_URL") or DEFAULT_POSTGRES_DSN


def ray_address() -> str:
    return env_text("CUBE_WEB_RAY_ADDRESS") or env_text("RAY_ADDRESS") or DEFAULT_RAY_ADDRESS


def load_demo_partition_schemas() -> bool:
    return bool_option(env_text("CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS"), False)


def require_postgres_dsn() -> str:
    value = postgres_dsn()
    if not value:
        raise RuntimeError("PostgreSQL DSN is required; set CUBE_WEB_POSTGRES_DSN, POSTGRES_DSN, or DATABASE_URL")
    return value


def require_ray_address() -> str:
    value = ray_address()
    if not value:
        raise RuntimeError("Ray address is required; set CUBE_WEB_RAY_ADDRESS or RAY_ADDRESS")
    return value


def minio_settings(options: dict[str, Any] | None = None) -> MinioSettings:
    raw = dict(options or {})
    service_env = minio_service_env()
    return MinioSettings(
        endpoint=str(
            raw.get("endpoint")
            or raw.get("minio_endpoint")
            or env_text("CUBE_WEB_MINIO_ENDPOINT")
            or env_text("MINIO_ENDPOINT")
            or DEFAULT_MINIO_ENDPOINT
        ),
        access_key=str(
            raw.get("access_key")
            or raw.get("minio_access_key")
            or env_text("CUBE_WEB_MINIO_ACCESS_KEY")
            or env_text("MINIO_ACCESS_KEY")
            or service_env.get("MINIO_ROOT_USER")
            or DEFAULT_MINIO_ACCESS_KEY
        ),
        secret_key=str(
            raw.get("secret_key")
            or raw.get("minio_secret_key")
            or env_text("CUBE_WEB_MINIO_SECRET_KEY")
            or env_text("MINIO_SECRET_KEY")
            or service_env.get("MINIO_ROOT_PASSWORD")
            or DEFAULT_MINIO_SECRET_KEY
        ),
        bucket=str(raw.get("bucket") or raw.get("minio_bucket") or env_text("CUBE_WEB_MINIO_BUCKET") or env_text("MINIO_BUCKET") or DEFAULT_MINIO_BUCKET),
        secure=bool_option(raw.get("secure", raw.get("minio_secure")), False),
    )


def auth_settings() -> AuthSettings:
    return AuthSettings(
        main_system_url=(env_text("CUBE_WEB_AUTH_MAIN_SYSTEM_URL") or DEFAULT_AUTH_MAIN_SYSTEM_URL).rstrip("/"),
        client_id=env_text("CUBE_WEB_AUTH_CLIENT_ID") or DEFAULT_AUTH_CLIENT_ID,
        client_secret=env_text("CUBE_WEB_AUTH_CLIENT_SECRET"),
        redirect_uri=env_text("CUBE_WEB_AUTH_REDIRECT_URI") or DEFAULT_AUTH_REDIRECT_URI,
        jwt_secret_key=env_text("CUBE_WEB_AUTH_JWT_SECRET_KEY"),
        jwt_algorithm=env_text("CUBE_WEB_AUTH_JWT_ALGORITHM") or "HS256",
        token_path=env_text("CUBE_WEB_AUTH_TOKEN_PATH") or DEFAULT_AUTH_TOKEN_PATH,
        logout_path=env_text("CUBE_WEB_AUTH_LOGOUT_PATH") or DEFAULT_AUTH_LOGOUT_PATH,
        required=bool_option(env_text("CUBE_WEB_AUTH_REQUIRED"), False),
    )


def portal_settings() -> PortalSettings:
    auth = auth_settings()
    main_url = env_text("CUBE_WEB_PORTAL_MAIN_URL") or auth.main_system_url
    home_url = env_text("CUBE_WEB_PORTAL_HOME_URL") or DEFAULT_PORTAL_HOME_URL
    partition_url = env_text("CUBE_WEB_PORTAL_PARTITION_SERVICE_URL") or DEFAULT_PORTAL_PARTITION_SERVICE_URL
    dispatch_url = env_text("CUBE_WEB_PORTAL_DISPATCH_URL") or DEFAULT_PORTAL_DISPATCH_URL
    data_ingest_url = env_text("CUBE_WEB_PORTAL_DATA_INGEST_URL") or join_url(main_url, DEFAULT_PORTAL_DATA_PATH) or DEFAULT_PORTAL_DATA_INGEST_URL
    admin_url = env_text("CUBE_WEB_PORTAL_ADMIN_URL") or join_url(main_url, DEFAULT_PORTAL_ADMIN_PATH) or DEFAULT_PORTAL_ADMIN_URL
    return PortalSettings(
        main_system_url=main_url,
        home_url=home_url,
        data_ingest_url=data_ingest_url,
        partition_service_url=partition_url,
        dispatch_url=dispatch_url,
        admin_url=admin_url,
    )


def navigation_items() -> list[dict[str, str]]:
    settings = portal_settings()
    items = [
        ("首页", settings.home_url),
        ("剖分数据服务", settings.partition_service_url),
        ("资源调度", settings.dispatch_url),
        ("ARD数据载入", settings.data_ingest_url),
        ("后台管理", settings.admin_url),
    ]
    return [{"label": label, "kind": "external", "url": url} for label, url in items if url]


def minio_service_env(path: Path | str = "/etc/default/minio") -> dict[str, str]:
    env_path = Path(path)
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def env_text(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    if value is None:
        value = local_env_values().get(name)
    if value is None:
        return default
    return value.strip() or default


def local_env_values() -> dict[str, str]:
    env_file = os.environ.get("CUBE_WEB_ENV_FILE")
    if env_file:
        return read_env_file(Path(env_file))
    for path in env_file_candidates():
        values = read_env_file(path)
        if values:
            return values
    return {}


def env_file_candidates() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [Path.cwd() / ".cube_web.env", repo_root / ".cube_web.env"]
    unique: list[Path] = []
    for path in candidates:
        if path not in unique:
            unique.append(path)
    return unique


def read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def join_url(base: str, path: str) -> str:
    if not base:
        return ""
    return urljoin(base.rstrip("/") + "/", path.lstrip("/"))


def bool_option(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
