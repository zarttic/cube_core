from pydantic import BaseModel


class AppConfig(BaseModel):
    app_name: str = "cube-encoder"
    api_prefix: str = "/v1"
    host: str = "0.0.0.0"
    port: int = 50012


config = AppConfig()
