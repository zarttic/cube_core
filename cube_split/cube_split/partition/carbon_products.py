from __future__ import annotations

from pathlib import Path
from typing import Protocol


class CarbonProductAdapter(Protocol):
    product_type: str
    aliases: tuple[str, ...]

    def supports_file(self, path: Path) -> bool:
        ...

    def load_observations(self, path: Path, max_observations: int | None = None) -> list[object]:
        ...


class XCO2ProductAdapter:
    product_type = "xco2"
    aliases = ("xco2", "oco2_lite", "oco2", "tansat_xco2")

    def supports_file(self, path: Path) -> bool:
        return path.suffix.lower() in {".jsonl", ".csv", ".nc", ".nc4", ".h5", ".hdf"}

    def load_observations(self, path: Path, max_observations: int | None = None) -> list[object]:
        from cube_split.partition.carbon import _load_xco2_observations_from_file

        return list(_load_xco2_observations_from_file(path, max_observations=max_observations))


_ADAPTERS: tuple[CarbonProductAdapter, ...] = (
    XCO2ProductAdapter(),
)


def supported_carbon_product_types() -> tuple[str, ...]:
    return tuple(adapter.product_type for adapter in _ADAPTERS)


def get_carbon_product_adapter(product_type: str) -> CarbonProductAdapter:
    normalized = product_type.strip().lower().replace("-", "_")
    for adapter in _ADAPTERS:
        if normalized == adapter.product_type or normalized in adapter.aliases:
            return adapter
    raise ValueError(f"Unsupported carbon product_type: {product_type}")


def normalize_carbon_product_type(product_type: str) -> str:
    return get_carbon_product_adapter(product_type).product_type
