from __future__ import annotations

BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
BASE32_DECODE = {c: i for i, c in enumerate(BASE32)}
BITS = [16, 8, 4, 2, 1]


class GeohashDecodeError(ValueError):
    pass


def bits_for_precision(precision: int) -> tuple[int, int]:
    total_bits = precision * 5
    lon_bits = (total_bits + 1) // 2
    lat_bits = total_bits // 2
    return lon_bits, lat_bits


def cell_size(precision: int) -> tuple[float, float]:
    lon_bits, lat_bits = bits_for_precision(precision)
    lon_step = 360.0 / (2**lon_bits)
    lat_step = 180.0 / (2**lat_bits)
    return lon_step, lat_step


def encode(lon: float, lat: float, precision: int = 12) -> str:
    lon_interval = [-180.0, 180.0]
    lat_interval = [-90.0, 90.0]
    geohash = []
    bit = 0
    ch = 0
    even = True

    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2.0
            if lon >= mid:
                ch |= BITS[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2.0
            if lat >= mid:
                ch |= BITS[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def decode_bbox(code: str) -> tuple[float, float, float, float]:
    lon_interval = [-180.0, 180.0]
    lat_interval = [-90.0, 90.0]
    even = True

    for char in code:
        if char not in BASE32_DECODE:
            raise GeohashDecodeError(f"Invalid geohash char: {char}")
        cd = BASE32_DECODE[char]
        for mask in BITS:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2.0
                if cd & mask:
                    lon_interval[0] = mid
                else:
                    lon_interval[1] = mid
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2.0
                if cd & mask:
                    lat_interval[0] = mid
                else:
                    lat_interval[1] = mid
            even = not even

    return lon_interval[0], lat_interval[0], lon_interval[1], lat_interval[1]


def decode_center(code: str) -> tuple[float, float]:
    min_lon, min_lat, max_lon, max_lat = decode_bbox(code)
    return (min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0


def polygon_from_bbox(bbox: tuple[float, float, float, float]) -> dict:
    min_lon, min_lat, max_lon, max_lat = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [min_lon, min_lat],
                [max_lon, min_lat],
                [max_lon, max_lat],
                [min_lon, max_lat],
                [min_lon, min_lat],
            ]
        ],
    }


def to_grid_index(lon: float, lat: float, precision: int) -> tuple[int, int]:
    lon_step, lat_step = cell_size(precision)
    lon_bins = 2 ** bits_for_precision(precision)[0]
    lat_bins = 2 ** bits_for_precision(precision)[1]
    ix = int((lon + 180.0) // lon_step)
    iy = int((lat + 90.0) // lat_step)
    ix = max(0, min(ix, lon_bins - 1))
    iy = max(0, min(iy, lat_bins - 1))
    return ix, iy


def from_grid_index(ix: int, iy: int, precision: int) -> str:
    lon_step, lat_step = cell_size(precision)
    lon = -180.0 + (ix + 0.5) * lon_step
    lat = -90.0 + (iy + 0.5) * lat_step
    return encode(lon, lat, precision=precision)


def neighbors(code: str, k: int = 1) -> list[str]:
    precision = len(code)
    lon_bins = 2 ** bits_for_precision(precision)[0]
    lat_bins = 2 ** bits_for_precision(precision)[1]
    lon, lat = decode_center(code)
    ix, iy = to_grid_index(lon, lat, precision)

    result: set[str] = set()
    for dx in range(-k, k + 1):
        for dy in range(-k, k + 1):
            if dx == 0 and dy == 0:
                continue
            nx = (ix + dx) % lon_bins
            ny = iy + dy
            if ny < 0 or ny >= lat_bins:
                continue
            result.add(from_grid_index(nx, ny, precision))
    return sorted(result)
