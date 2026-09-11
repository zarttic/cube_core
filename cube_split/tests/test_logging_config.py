from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler

import pytest

from cube_split import logging_config, runtime_config

_LOG_ENV_VARS = (
    "CUBE_LOG_LEVEL",
    "CUBE_LOG_FORMAT",
    "CUBE_LOG_FILE",
    "CUBE_LOG_MAX_BYTES",
    "CUBE_LOG_BACKUP_COUNT",
    "CUBE_LOG_RAY_LEVEL",
    "CUBE_LOG_ACCESS",
)


@pytest.fixture(autouse=True)
def isolated_logging_env(monkeypatch, tmp_path):
    """Neutralize ambient env files and restore root handlers after every test."""
    empty_env = tmp_path / "empty.env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(empty_env))
    for name in _LOG_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    root = logging.getLogger()
    previous_level = root.level
    access_logger = logging.getLogger("uvicorn.access")
    previous_access_level = access_logger.level
    yield
    for handler in list(root.handlers):
        if getattr(handler, "_cube_log_handler", False):
            root.removeHandler(handler)
            handler.close()
    root.setLevel(previous_level)
    access_logger.setLevel(previous_access_level)


def _owned_handlers() -> list[logging.Handler]:
    return [h for h in logging.getLogger().handlers if getattr(h, "_cube_log_handler", False)]


def _read_log_lines(path) -> list[dict]:
    for handler in _owned_handlers():
        handler.flush()
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_configure_logging_is_idempotent():
    assert logging_config.configure_logging(service="test-service", force=True) is True
    assert logging_config.configure_logging(service="test-service") is False
    assert len(_owned_handlers()) == 1
    assert isinstance(_owned_handlers()[0], logging.StreamHandler)


def test_configure_logging_writes_json_with_context(monkeypatch, tmp_path):
    target = tmp_path / "logs" / "cube.log"
    monkeypatch.setenv("CUBE_LOG_FORMAT", "json")
    monkeypatch.setenv("CUBE_LOG_FILE", str(target))
    monkeypatch.setenv("CUBE_LOG_LEVEL", "INFO")

    assert logging_config.configure_logging(service="cube-web", force=True) is True
    with logging_config.logging_context(task_id="task-01", actor="alice"):
        logging.getLogger("cube_web.tests").info("partition.task_started")

    records = _read_log_lines(target)
    starts = [record for record in records if record["message"] == "partition.task_started"]
    assert len(starts) == 1
    record = starts[0]
    assert record["level"] == "INFO"
    assert record["logger"] == "cube_web.tests"
    assert record["service"] == "cube-web"
    assert record["context"] == {"task_id": "task-01", "actor": "alice"}
    assert record["pid"] > 0


def test_configure_logging_reports_invalid_level(monkeypatch, tmp_path):
    target = tmp_path / "cube.log"
    monkeypatch.setenv("CUBE_LOG_FILE", str(target))
    monkeypatch.setenv("CUBE_LOG_LEVEL", "VERBOSE")

    logging_config.configure_logging(service="cube-web", force=True)

    lines = target.read_text(encoding="utf-8").splitlines()
    assert logging.getLogger().level == logging.INFO
    assert any("logging.invalid_level" in line and "VERBOSE" in line for line in lines)


def test_configure_logging_builds_rotating_file_handler(monkeypatch, tmp_path):
    target = tmp_path / "cube.log"
    monkeypatch.setenv("CUBE_LOG_FILE", str(target))
    monkeypatch.setenv("CUBE_LOG_MAX_BYTES", "1024")
    monkeypatch.setenv("CUBE_LOG_BACKUP_COUNT", "2")

    logging_config.configure_logging(service="cube-web", force=True)

    handler = _owned_handlers()[0]
    assert isinstance(handler, RotatingFileHandler)
    assert handler.maxBytes == 1024
    assert handler.backupCount == 2


def test_context_survives_across_records_and_cleans_up():
    logging_config.configure_logging(service="test", force=True)
    assert logging_config.current_context() == {}
    with logging_config.logging_context(task_id="task-02"):
        assert logging_config.current_context()["task_id"] == "task-02"
    assert logging_config.current_context() == {}


def test_context_sanitizes_newlines_and_truncates():
    logging_config.configure_logging(service="test", force=True)
    with logging_config.logging_context(detail="line1\nline2", blob="x" * 500):
        context = logging_config.current_context()
    assert context["detail"] == "line1 line2"
    assert len(context["blob"]) == logging_config.DEFAULT_MAX_FIELD_LENGTH


def test_logging_env_vars_excludes_file(monkeypatch):
    monkeypatch.setenv("CUBE_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("CUBE_LOG_FILE", "/tmp/worker-should-not-write.log")

    env_vars = logging_config.logging_env_vars()

    assert env_vars["CUBE_LOG_LEVEL"] == "DEBUG"
    assert "CUBE_LOG_FILE" not in env_vars


def test_apply_ray_logging_defaults_keeps_operator_values(monkeypatch):
    monkeypatch.setenv("CUBE_LOG_LEVEL", "INFO")
    runtime_env = {
        "env_vars": {"CUBE_LOG_LEVEL": "WARNING"},
        "worker_process_setup_hook": "custom.module.hook",
    }

    merged = logging_config.apply_ray_logging_defaults(runtime_env)

    assert merged["env_vars"]["CUBE_LOG_LEVEL"] == "WARNING"
    assert merged["env_vars"]["CUBE_LOG_FORMAT"] == "text"
    assert merged["worker_process_setup_hook"] == "custom.module.hook"


def test_apply_ray_logging_defaults_adds_hook(monkeypatch):
    monkeypatch.delenv("CUBE_LOG_RAY_LEVEL", raising=False)
    runtime_env: dict = {}

    merged = logging_config.apply_ray_logging_defaults(runtime_env)

    assert merged["worker_process_setup_hook"] == logging_config.WORKER_PROCESS_SETUP_HOOK
    assert merged["env_vars"]["CUBE_LOG_RAY_LEVEL"] == "ERROR"


def test_ray_logging_level_follows_env(monkeypatch):
    assert logging_config.ray_logging_level() == logging.ERROR
    monkeypatch.setenv("CUBE_LOG_RAY_LEVEL", "warning")
    assert logging_config.ray_logging_level() == logging.WARNING


def test_worker_setup_never_raises(monkeypatch):
    def _boom(**_: object) -> bool:
        raise RuntimeError("no logging for you")

    monkeypatch.setattr(logging_config, "configure_logging", _boom)

    logging_config.worker_setup()


def test_env_text_file_is_not_read_for_ray_level(monkeypatch, tmp_path):
    env_file = tmp_path / "cube_web.env"
    env_file.write_text("CUBE_LOG_RAY_LEVEL=DEBUG\n", encoding="utf-8")
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(env_file))
    monkeypatch.delenv("CUBE_LOG_RAY_LEVEL", raising=False)

    assert runtime_config.env_text("CUBE_LOG_RAY_LEVEL") == "DEBUG"
    assert logging_config.ray_logging_level() == logging.DEBUG


def test_context_nesting_accumulates():
    logging_config.configure_logging(service="test", force=True)
    with logging_config.logging_context(task_id="task-03"):
        with logging_config.logging_context(asset_id="asset-9"):
            context = logging_config.current_context()
    assert context == {"task_id": "task-03", "asset_id": "asset-9"}
    assert logging_config.current_context() == {}
