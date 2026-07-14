from __future__ import annotations

from datetime import datetime

from grid_core.app.core.enums import GridType, TimeGranularity
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.st_code import STCode
from grid_core.app.utils.timecode import to_time_code
from grid_core.app.utils.validator import parse_st_code

# M1 frozen contract: only the three production grid types.
PREFIX_MAP = {
    GridType.GEOHASH: "gh",
    GridType.MGRS: "mgrs",
    GridType.ISEA4H: "i4h",
}
PREFIX_MAP_REVERSE = {v: k for k, v in PREFIX_MAP.items()}
TIME_CODE_FORMATS_BY_LENGTH = {
    6: "%Y%m",
    8: "%Y%m%d",
    10: "%Y%m%d%H",
    12: "%Y%m%d%H%M",
    14: "%Y%m%d%H%M%S",
}


class CodeService:
    def generate_st_code(
        self,
        address: GridAddress,
        timestamp: datetime,
        time_granularity: TimeGranularity,
    ) -> STCode:
        grid_type = GridType(address.grid_type)
        if grid_type not in PREFIX_MAP:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        time_code = to_time_code(timestamp, time_granularity)
        return self.build_st_code(
            grid_type=grid_type,
            grid_level=address.grid_level,
            space_code=address.space_code,
            time_code=time_code,
        )

    def build_st_code(
        self,
        grid_type: GridType,
        grid_level: int,
        space_code: str,
        time_code: str,
    ) -> STCode:
        if grid_type not in PREFIX_MAP:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        prefix = PREFIX_MAP[grid_type]
        st_code_str = f"{prefix}:{grid_level}:{space_code}:{time_code}"
        return STCode(
            grid_type=grid_type.value,
            grid_level=grid_level,
            space_code=space_code,
            time_code=time_code,
            st_code=st_code_str,
        )

    def parse_st_code(self, st_code: str) -> STCode:
        parsed = parse_st_code(st_code)
        prefix = parsed["prefix"]
        if prefix not in PREFIX_MAP_REVERSE:
            raise ValidationError(f"Unsupported grid prefix: {prefix}")
        grid_type = PREFIX_MAP_REVERSE[prefix]
        self._validate_time_code(parsed["time_code"])
        return STCode(
            grid_type=grid_type.value,
            grid_level=parsed["grid_level"],
            space_code=parsed["space_code"],
            time_code=parsed["time_code"],
            st_code=st_code,
        )

    def batch_generate_st_codes(
        self,
        grid_type: GridType,
        grid_level: int,
        items: list[dict],
        time_granularity: TimeGranularity,
    ) -> list[str]:
        st_codes: list[str] = []
        for item in items:
            address = GridAddress(grid_type=grid_type.value, grid_level=grid_level, space_code=item["space_code"])
            result = self.generate_st_code(
                address=address,
                timestamp=item["timestamp"],
                time_granularity=time_granularity,
            )
            st_codes.append(result.st_code)
        return st_codes

    @staticmethod
    def _validate_time_code(time_code: str) -> None:
        fmt = TIME_CODE_FORMATS_BY_LENGTH.get(len(time_code))
        if fmt is None:
            raise ValidationError("Invalid time_code length")
        try:
            datetime.strptime(time_code, fmt)
        except ValueError as exc:
            raise ValidationError("Invalid time_code value") from exc
