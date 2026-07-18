from __future__ import annotations

from cube_split import runtime_config


def test_env_text_reads_local_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "cube_web.env"
    env_file.write_text(
        "\n".join(
            [
                "CUBE_WEB_POSTGRES_DSN=postgresql://local/cube",
                "CUBE_WEB_RAY_ADDRESS=10.3.100.182:6379",
                "CUBE_WEB_MINIO_ENDPOINT=10.3.100.179:9000",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("CUBE_WEB_POSTGRES_DSN", raising=False)
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(env_file))

    assert runtime_config.postgres_dsn() == "postgresql://local/cube"
    assert runtime_config.ray_address() == "10.3.100.182:6379"
    assert runtime_config.minio_settings().endpoint == "10.3.100.179:9000"


def test_environment_overrides_local_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "cube_web.env"
    env_file.write_text("CUBE_WEB_RAY_ADDRESS=ray://from-file:10001\n", encoding="utf-8")
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(env_file))
    monkeypatch.setenv("CUBE_WEB_RAY_ADDRESS", "ray://from-env:10001")

    assert runtime_config.ray_address() == "ray://from-env:10001"


def test_auth_is_required_when_runtime_value_is_not_configured(monkeypatch):
    monkeypatch.setattr(runtime_config, "env_text", lambda _name, default="": default)

    assert runtime_config.auth_settings().required is True
