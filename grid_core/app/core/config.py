from pydantic import BaseModel


class AppConfig(BaseModel):
    app_name: str = "cube-encoder"
    api_prefix: str = "/v1"


config = AppConfig()
