import re

from grid_core.app.core.exceptions import ParseError


ST_CODE_PATTERN = re.compile(
    r"^(?P<prefix>[a-z0-9]+):(?P<level>\d+):(?P<space_code>[^:]+):(?P<time_code>\d+):(?P<version>v\d+)$"
)


def parse_st_code(st_code: str) -> dict:
    match = ST_CODE_PATTERN.match(st_code)
    if not match:
        raise ParseError(f"Invalid st_code format: {st_code}")
    data = match.groupdict()
    data["level"] = int(data["level"])
    return data
