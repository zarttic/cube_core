from __future__ import annotations

import re
from datetime import datetime

import mgrs
from s2sphere import CellId

from grid_core.app.core.enums import GridType, TimeGranularity
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.st_code import STCode
from grid_core.app.utils.timecode import to_time_code
from grid_core.app.utils.validator import parse_st_code

PREFIX_MAP = {
    GridType.S2: "s2",
    GridType.MGRS: "mgrs",
    GridType.ISEA4H: "hx",
    GridType.TILE_MATRIX: "tm",
    GridType.PLANE_GRID: "pg",
}
PREFIX_MAP_REVERSE = {v: k for k, v in PREFIX_MAP.items()}
MGRS_CONVERTER = mgrs.MGRS()
PLANE_GRID_CRS_TOKEN_RE = re.compile(r"^[a-z0-9_]+$")
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
        grid_type: GridType,
        level: int,
        space_code: str,
        timestamp,
        time_granularity: TimeGranularity,
    ) -> STCode:
        if grid_type not in PREFIX_MAP:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        time_code = to_time_code(timestamp, time_granularity)
        self._validate_space_code_and_level(grid_type=grid_type, level=level, space_code=space_code)
        return self.build_st_code(
            grid_type=grid_type,
            level=level,
            space_code=space_code,
            time_code=time_code,
        )

    def build_st_code(
        self,
        grid_type: GridType,
        level: int,
        space_code: str,
        time_code: str,
    ) -> STCode:
        if grid_type not in PREFIX_MAP:
            raise ValidationError(f"Unsupported grid_type: {grid_type}")
        prefix = PREFIX_MAP[grid_type]
        st_code = f"{prefix}:{level}:{space_code}:{time_code}"
        return STCode(
            grid_type=grid_type.value,
            level=level,
            space_code=space_code,
            time_code=time_code,
            st_code=st_code,
        )

    def parse_st_code(self, st_code: str) -> STCode:
        parsed = parse_st_code(st_code)
        prefix = parsed["prefix"]
        if prefix not in PREFIX_MAP_REVERSE:
            raise ValidationError(f"Unsupported grid prefix: {prefix}")
        grid_type = PREFIX_MAP_REVERSE[prefix]
        self._validate_space_code_and_level(
            grid_type=grid_type,
            level=parsed["level"],
            space_code=parsed["space_code"],
        )
        self._validate_time_code(parsed["time_code"])
        return STCode(
            grid_type=grid_type.value,
            level=parsed["level"],
            space_code=parsed["space_code"],
            time_code=parsed["time_code"],
            st_code=st_code,
        )

    def batch_generate_st_codes(
        self,
        grid_type: GridType,
        level: int,
        items: list[dict],
        time_granularity: TimeGranularity,
    ) -> list[str]:
        st_codes: list[str] = []
        for item in items:
            result = self.generate_st_code(
                grid_type=grid_type,
                level=level,
                space_code=item["space_code"],
                timestamp=item["timestamp"],
                time_granularity=time_granularity,
            )
            st_codes.append(result.st_code)
        return st_codes

    @staticmethod
    def _validate_space_code_and_level(grid_type: GridType, level: int, space_code: str) -> None:
        if level < 1:
            raise ValidationError("level must be >= 1")

        if grid_type == GridType.S2:
            if level > 12:
                raise ValidationError("S2 level must be in [1, 12]")
            try:
                cid = CellId.from_token(space_code)
            except Exception as exc:
                raise ValidationError("Invalid s2 space_code") from exc
            if not cid.is_valid():
                raise ValidationError("Invalid s2 space_code")
            if cid.level() != level:
                raise ValidationError("S2 space_code level does not match level")
            return

        if grid_type == GridType.MGRS:
            if len(space_code) < 5:
                raise ValidationError("Invalid MGRS space_code")
            suffix_len = len(space_code) - 5
            if suffix_len % 2 != 0:
                raise ValidationError("Invalid MGRS precision digits")
            precision = suffix_len // 2
            actual_level = precision + 1
            if actual_level != level:
                raise ValidationError("MGRS space_code level does not match level")
            if precision < 0 or precision > 5:
                raise ValidationError("MGRS level must be in [1, 6]")
            try:
                MGRS_CONVERTER.MGRSToUTM(space_code)
            except Exception as exc:
                raise ValidationError("Invalid MGRS space_code") from exc
            return

        if grid_type == GridType.ISEA4H:
            import h3

            if not h3.is_valid_cell(space_code):
                raise ValidationError("Invalid ISEA4H space_code")
            actual_level = h3.get_resolution(space_code)
            if actual_level != level:
                raise ValidationError("ISEA4H space_code level does not match level")
            if level > 12:
                raise ValidationError("ISEA4H level must be in [1, 12]")
            return

        if grid_type == GridType.TILE_MATRIX:
            try:
                actual_level, x, y = [int(part) for part in space_code.split("/")]
            except Exception as exc:
                raise ValidationError("Invalid tile_matrix space_code") from exc
            if actual_level != level:
                raise ValidationError("tile_matrix space_code level does not match level")
            if level > 12:
                raise ValidationError("tile_matrix level must be in [1, 12]")
            matrix_width = 2 ** (level + 1)
            matrix_height = 2**level
            if x < 0 or x >= matrix_width or y < 0 or y >= matrix_height:
                raise ValidationError("tile_matrix space_code row or column out of range")
            return

        if grid_type == GridType.PLANE_GRID:
            try:
                crs_token, actual_level_text, col_text, row_text = space_code.split("/")
                actual_level = int(actual_level_text)
                col = int(col_text)
                row = int(row_text)
            except Exception as exc:
                raise ValidationError("Invalid plane_grid space_code") from exc
            if actual_level != level:
                raise ValidationError("plane_grid space_code level does not match level")
            if not PLANE_GRID_CRS_TOKEN_RE.match(crs_token):
                raise ValidationError("Invalid plane_grid CRS token")
            if col < 0 or row < 0:
                raise ValidationError("plane_grid space_code row or column out of range")
            return

        raise ValidationError(f"Unsupported grid_type: {grid_type}")

    @staticmethod
    def _validate_time_code(time_code: str) -> None:
        fmt = TIME_CODE_FORMATS_BY_LENGTH.get(len(time_code))
        if fmt is None:
            raise ValidationError("Invalid time_code length")
        try:
            datetime.strptime(time_code, fmt)
        except ValueError as exc:
            raise ValidationError("Invalid time_code value") from exc
