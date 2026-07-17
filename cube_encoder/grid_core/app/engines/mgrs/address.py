"""Canonical MGRS address helpers: canonicalization, topology code building and parsing."""
from __future__ import annotations

import re
from dataclasses import dataclass

import mgrs as mgrs_lib

from grid_core.app.core.exceptions import ValidationError

# Regex for full topology code validation/parsing
MGRS_TOPOLOGY_RE = re.compile(
    r"^mgrs-topo-v1:(utm-(?:[1-9]|[1-5][0-9]|60)[ns]|ups-[ns]):([0-5]):([0-9A-Z]{3,15})$"
)

_converter = mgrs_lib.MGRS()


def canonicalize_mgrs(code: str) -> str:
    """Strip whitespace, uppercase, verify the code is parseable, and return it."""
    stripped = code.replace(" ", "").upper()
    if not stripped:
        raise ValidationError("MGRS code is empty")
    # Verify it's valid by round-tripping; UPS codes will fail MGRSToUTM but toLatLon works
    try:
        _converter.toLatLon(stripped)
    except Exception as exc:
        raise ValidationError(f"Invalid MGRS code: {code!r}") from exc
    return stripped


def suffix_digit_count(code: str) -> int:
    """Count trailing numeric digits in the MGRS code (must be even, 0-10)."""
    count = 0
    for ch in reversed(code):
        if ch.isdigit():
            count += 1
        else:
            break
    return count


def precision_from_code(code: str) -> int:
    """Derive MGRS precision 0-5 from trailing digit count."""
    n = suffix_digit_count(code)
    if n % 2 != 0:
        raise ValidationError(f"MGRS code has odd digit suffix ({n} digits): {code!r}")
    p = n // 2
    if p < 0 or p > 5:
        raise ValidationError(f"MGRS precision {p} out of range [0, 5]: {code!r}")
    return p


def parent_space_code(code: str) -> str:
    """Return the standard MGRS parent by removing one easting and northing digit."""
    canonical = canonicalize_mgrs(code)
    precision = precision_from_code(canonical)
    if precision == 0:
        raise ValidationError("MGRS precision 0 has no parent")
    base = canonical[: -2 * precision]
    digits = canonical[-2 * precision :]
    east, north = digits[:precision], digits[precision:]
    return f"{base}{east[:-1]}{north[:-1]}"


def direct_child_space_codes(code: str) -> list[str]:
    """Return the 100 syntactic direct children of a standard MGRS code."""
    canonical = canonicalize_mgrs(code)
    precision = precision_from_code(canonical)
    if precision >= 5:
        raise ValidationError("MGRS precision 5 has no children")
    base = canonical if precision == 0 else canonical[: -2 * precision]
    digits = "" if precision == 0 else canonical[-2 * precision :]
    east, north = digits[:precision], digits[precision:]
    return [
        f"{base}{east}{e}{north}{n}"
        for e in range(10)
        for n in range(10)
    ]


@dataclass(frozen=True)
class ParsedTopologyCode:
    domain_token: str
    level: int
    space_code: str


def parse_topology_code(topology_code: str) -> ParsedTopologyCode:
    """Parse a well-formed MGRS topology code into its components."""
    m = MGRS_TOPOLOGY_RE.match(topology_code)
    if not m:
        raise ValidationError(f"Invalid MGRS topology code: {topology_code!r}")
    return ParsedTopologyCode(
        domain_token=m.group(1),
        level=int(m.group(2)),
        space_code=m.group(3),
    )


def build_topology_code(domain_token: str, level: int, space_code: str) -> str:
    """Build the canonical mgrs-topo-v1 topology code string.

    Raises ValidationError if the result would exceed 96 ASCII bytes.
    """
    canonical = space_code.replace(" ", "").upper()
    value = f"mgrs-topo-v1:{domain_token}:{level}:{canonical}"
    if len(value.encode("ascii")) > 96:
        raise ValidationError(
            f"MGRS topology_code exceeds 96 ASCII bytes: {value!r}"
        )
    return value
