from pydantic import BaseModel


class STCode(BaseModel):
    """ST (Space-Time) code combining a grid address with a time code.

    Fields match the stable public SDK contract:
      grid_type, grid_level, space_code, time_code, st_code
    """

    grid_type: str
    grid_level: int
    space_code: str
    time_code: str
    st_code: str
