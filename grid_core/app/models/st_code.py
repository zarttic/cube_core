from pydantic import BaseModel


class STCode(BaseModel):
    grid_type: str
    level: int
    space_code: str
    time_code: str
    version: str
    st_code: str
