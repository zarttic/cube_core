from grid_core.app.core.enums import GridType, TimeGranularity
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.st_code import STCode
from grid_core.app.utils.timecode import to_time_code
from grid_core.app.utils.validator import parse_st_code


PREFIX_MAP = {
    GridType.GEOHASH: "gh",
    GridType.MGRS: "mgrs",
    GridType.ISEA4H: "hx",
}
PREFIX_MAP_REVERSE = {v: k for k, v in PREFIX_MAP.items()}


class CodeService:
    def generate_st_code(
        self,
        grid_type: GridType,
        level: int,
        space_code: str,
        timestamp,
        time_granularity: TimeGranularity,
        version: str,
    ) -> STCode:
        if grid_type not in PREFIX_MAP:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        time_code = to_time_code(timestamp, time_granularity)
        prefix = PREFIX_MAP[grid_type]
        st_code = f"{prefix}:{level}:{space_code}:{time_code}:{version}"
        return STCode(
            grid_type=grid_type.value,
            level=level,
            space_code=space_code,
            time_code=time_code,
            version=version,
            st_code=st_code,
        )

    def parse_st_code(self, st_code: str) -> STCode:
        parsed = parse_st_code(st_code)
        prefix = parsed["prefix"]
        if prefix not in PREFIX_MAP_REVERSE:
            raise ValidationError(f"Unsupported grid prefix: {prefix}")
        grid_type = PREFIX_MAP_REVERSE[prefix]
        return STCode(
            grid_type=grid_type.value,
            level=parsed["level"],
            space_code=parsed["space_code"],
            time_code=parsed["time_code"],
            version=parsed["version"],
            st_code=st_code,
        )

    def batch_generate_st_codes(
        self,
        grid_type: GridType,
        level: int,
        items: list[dict],
        time_granularity: TimeGranularity,
        version: str,
    ) -> list[str]:
        st_codes: list[str] = []
        for item in items:
            result = self.generate_st_code(
                grid_type=grid_type,
                level=level,
                space_code=item["space_code"],
                timestamp=item["timestamp"],
                time_granularity=time_granularity,
                version=version,
            )
            st_codes.append(result.st_code)
        return st_codes
