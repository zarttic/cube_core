"""Role normalization for dataset visibility controls."""

from __future__ import annotations

DATASET_ROLES = ("NORMAL", "ADVANCED", "SCIENTIST", "ADMIN")
HIDEABLE_DATASET_ROLES = ("NORMAL", "ADVANCED", "SCIENTIST")
ROLE_LABELS = {
    "NORMAL": "普通用户",
    "ADVANCED": "高级用户",
    "SCIENTIST": "科学家团队",
    "ADMIN": "管理员",
}
_ROLE_ALIASES = {
    "normal": "NORMAL", "普通用户": "NORMAL",
    "advanced": "ADVANCED", "高级用户": "ADVANCED",
    "scientist": "SCIENTIST", "科学家团队": "SCIENTIST",
    "admin": "ADMIN", "administrator": "ADMIN", "管理员": "ADMIN",
}


def normalize_role(value: str | None) -> str:
    """Return the stored role code for Chinese, legacy, and enum token values."""
    text = str(value or "").strip()
    canonical = _ROLE_ALIASES.get(text.casefold()) or _ROLE_ALIASES.get(text)
    if canonical is None:
        raise ValueError(f"unsupported role: {value}")
    return canonical


def is_admin_role(value: str | None) -> bool:
    try:
        return normalize_role(value) == "ADMIN"
    except ValueError:
        return False


def viewer_role(value: str | None) -> str:
    """Treat unknown authenticated roles as ordinary users for dataset reads."""
    try:
        return normalize_role(value)
    except ValueError:
        return "NORMAL"
