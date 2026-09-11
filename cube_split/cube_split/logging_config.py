"""Process-wide logging setup shared by the web service, Ray drivers, and Ray workers.

The module is intentionally dependency-free (stdlib only) and idempotent so it can be
called from an application import, a Ray Job entrypoint, and a Ray worker setup hook
without stacking duplicate handlers.

Runtime switches (resolved through :mod:`cube_split.runtime_config`, so a local
``.cube_web.env`` works as well as real environment variables):

- ``CUBE_LOG_LEVEL``: root level, default ``INFO``.
- ``CUBE_LOG_FORMAT``: ``text`` (default) or ``json``.
- ``CUBE_LOG_FILE``: when set, write to that file with size-based rotation instead of
  stdout. Worker processes never inherit this value.
- ``CUBE_LOG_MAX_BYTES`` / ``CUBE_LOG_BACKUP_COUNT``: rotation controls.
- ``CUBE_LOG_RAY_LEVEL``: Ray's own ``logging_level``, default ``ERROR``.
- ``CUBE_LOG_ACCESS``: when ``0``, keeps uvicorn's access log untouched.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator

from cube_split import runtime_config

DEFAULT_LEVEL = "INFO"
DEFAULT_FORMAT = "text"
DEFAULT_SERVICE = "cube"
DEFAULT_RAY_LEVEL = "ERROR"
DEFAULT_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3
DEFAULT_MAX_FIELD_LENGTH = 200

#: Module path consumed by Ray's ``worker_process_setup_hook`` runtime_env field.
WORKER_PROCESS_SETUP_HOOK = "cube_split.logging_config.worker_setup"

_HANDLER_FLAG = "_cube_log_handler"
_LEVELS: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}
_CONTEXT: ContextVar[dict[str, str] | None] = ContextVar("cube_log_context", default=None)


def _clean_field(value: Any) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    if len(text) > DEFAULT_MAX_FIELD_LENGTH:
        text = text[: DEFAULT_MAX_FIELD_LENGTH - 3] + "..."
    return text


def current_context() -> dict[str, str]:
    """Return a copy of the fields bound to the current execution context."""
    return dict(_CONTEXT.get() or {})


@contextmanager
def logging_context(**fields: Any) -> Iterator[None]:
    """Add ``fields`` to the context of every log record emitted inside the block.

    Nested blocks accumulate fields instead of replacing them.
    """
    merged = current_context()
    merged.update({key: _clean_field(value) for key, value in fields.items() if value is not None})
    token = _CONTEXT.set(merged)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


class _ContextFilter(logging.Filter):
    """Attach the context fields plus the emitting service and pid to each record."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        context = current_context()
        context["service"] = self._service
        context["pid"] = str(os.getpid())
        record.cube_context = context
        return True


class _TextFormatter(logging.Formatter):
    """Single-line text format: ``ts LEVEL logger [k=v ...] message``."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record)
        context = getattr(record, "cube_context", None) or {}
        parts = [timestamp, record.levelname, record.name]
        if context:
            parts.append(" ".join(f"{key}={value}" for key, value in context.items()))
        line = " ".join(parts) + " " + record.getMessage()
        if record.exc_info:
            line = line + "\n" + self.formatException(record.exc_info)
        if record.stack_info:
            line = line + "\n" + self.formatStack(record.stack_info)
        return line


class _JsonFormatter(logging.Formatter):
    """One JSON object per line, suitable for log collectors."""

    def format(self, record: logging.LogRecord) -> str:
        context = getattr(record, "cube_context", None) or {}
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created).astimezone().isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": context.get("service"),
            "pid": record.process,
            "thread": record.threadName,
        }
        extra_context = {key: value for key, value in context.items() if key not in {"service", "pid"}}
        if extra_context:
            payload["context"] = extra_context
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _int_env(name: str, default: int) -> int:
    raw = runtime_config.env_text(name, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _resolve_level(name: str, default: str) -> tuple[int, str | None]:
    raw = runtime_config.env_text(name, default).upper()
    if raw in _LEVELS:
        return _LEVELS[raw], None
    return _LEVELS[default], raw


def _build_handler(log_format: str, log_file: str, service: str) -> logging.Handler:
    if log_file:
        path = Path(log_file).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        handler: logging.Handler = RotatingFileHandler(
            path,
            maxBytes=_int_env("CUBE_LOG_MAX_BYTES", DEFAULT_MAX_BYTES),
            backupCount=_int_env("CUBE_LOG_BACKUP_COUNT", DEFAULT_BACKUP_COUNT),
            encoding="utf-8",
        )
    else:
        handler = logging.StreamHandler()
    formatter: logging.Formatter = _JsonFormatter() if log_format == "json" else _TextFormatter()
    handler.setFormatter(formatter)
    setattr(handler, _HANDLER_FLAG, True)
    return handler


def configure_logging(*, service: str = DEFAULT_SERVICE, force: bool = False) -> bool:
    """Install the shared root handler. Returns ``False`` when already configured."""
    root = logging.getLogger()
    existing = [handler for handler in root.handlers if getattr(handler, _HANDLER_FLAG, False)]
    if existing and not force:
        return False
    for handler in existing:
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # pragma: no cover - defensive cleanup
            pass

    level, invalid_level = _resolve_level("CUBE_LOG_LEVEL", DEFAULT_LEVEL)
    log_format = runtime_config.env_text("CUBE_LOG_FORMAT", DEFAULT_FORMAT).strip().lower()
    if log_format not in {"text", "json"}:
        log_format = DEFAULT_FORMAT
    log_file = runtime_config.env_text("CUBE_LOG_FILE", "").strip()

    handler = _build_handler(log_format, log_file, service)
    handler.addFilter(_ContextFilter(service))
    root.addHandler(handler)
    root.setLevel(level)

    if runtime_config.bool_option(runtime_config.env_text("CUBE_LOG_ACCESS", "1"), True):
        # The application access middleware emits a richer line with request_id/actor.
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    with logging_context(
        level=logging.getLevelName(level),
        format=log_format,
        dest=log_file or "stdout",
    ):
        logger.info("logging.configured")
        if invalid_level is not None:
            logger.warning("logging.invalid_level value=%s fallback=%s", invalid_level, DEFAULT_LEVEL)
    return True


def worker_setup() -> None:
    """Ray ``worker_process_setup_hook`` entry point. Must never raise."""
    try:
        configure_logging(service="cube-ray-worker")
    except Exception as exc:  # pragma: no cover - defensive; a raise would kill workers
        print(f"[cube_split] worker logging setup failed: {exc}", file=sys.stderr)


def logging_env_vars() -> dict[str, str]:
    """Values propagated to Ray workers through ``runtime_env.env_vars``.

    ``CUBE_LOG_FILE`` is deliberately excluded: workers on different nodes must not
    write to a shared or node-local file path implicitly.
    """
    return {
        "CUBE_LOG_LEVEL": runtime_config.env_text("CUBE_LOG_LEVEL", DEFAULT_LEVEL),
        "CUBE_LOG_FORMAT": runtime_config.env_text("CUBE_LOG_FORMAT", DEFAULT_FORMAT),
        "CUBE_LOG_RAY_LEVEL": runtime_config.env_text("CUBE_LOG_RAY_LEVEL", DEFAULT_RAY_LEVEL),
    }


def ray_logging_level() -> int:
    """Ray's own ``logging_level``; default stays ``ERROR`` to preserve current noise."""
    level, _ = _resolve_level("CUBE_LOG_RAY_LEVEL", DEFAULT_RAY_LEVEL)
    return level


def apply_ray_logging_defaults(runtime_env: dict[str, Any]) -> dict[str, Any]:
    """Merge logging env vars and the worker setup hook without overriding operators."""
    env_vars = runtime_env.get("env_vars")
    if not isinstance(env_vars, dict):
        env_vars = {}
        runtime_env["env_vars"] = env_vars
    for name, value in logging_env_vars().items():
        env_vars.setdefault(name, value)
    runtime_env.setdefault("worker_process_setup_hook", WORKER_PROCESS_SETUP_HOOK)
    return runtime_env
