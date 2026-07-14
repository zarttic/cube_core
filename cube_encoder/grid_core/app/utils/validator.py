import re

from grid_core.app.core.exceptions import ParseError

# M1 contract: ST codes use canonical prefixes gh / mgrs / i4h (see
# grid_core.app.services.code_service.PREFIX_MAP). The numeric segment is
# grid_level, matching the GridAddress/STCode field name.
ST_CODE_PATTERN = re.compile(
    r"^(?P<prefix>[a-z0-9]+):(?P<grid_level>\d+):(?P<space_code>[^:]+):(?P<time_code>\d+)$"
)


def parse_st_code(st_code: str) -> dict:
    match = ST_CODE_PATTERN.match(st_code)
    if not match:
        raise ParseError(f"Invalid st_code format: {st_code}")
    data = match.groupdict()
    data["grid_level"] = int(data["grid_level"])
    return data
