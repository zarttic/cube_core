#!/usr/bin/env python3
"""Fail closed on stale active-tree M1-M5 partition contract references."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


EXCLUDED_PREFIXES = ("docs/superpowers/", ".git/", ".claude/", "node_modules/")
TOOL_TEST_EXCLUSIONS = frozenset(
    {"scripts/m5_cleanup_acceptance_scan.py", "tests/test_m5_cleanup_acceptance_scan.py"}
)
TOOL_OUTPUT_PREFIXES = ("artifacts/",)
RULE_NAMES = frozenset(
    {
        "legacy_grid",
        "cog_conversion",
        "legacy_request_level",
        "isea_h3_runtime_dependency",
        "forbidden_published_status",
    }
)
TEXT_SUFFIXES = frozenset(
    {
        ".cfg", ".css", ".csv", ".html", ".ini", ".js", ".json", ".lock", ".md",
        ".mjs", ".py", ".rst", ".sh", ".toml", ".ts", ".txt", ".vue", ".xml", ".yaml", ".yml",
    }
)
HISTORICAL_PATHS = frozenset(
    {
        "cube_split/docs/ENTITY_PARTITION_PERFORMANCE.md",
        "cube_split/docs/LOGICAL_PARTITION_PERFORMANCE.md",
        "cube_split/docs/PARTITION_OPTIMIZATION_REAL_DATA_TEST_REPORT.md",
        "cube_split/docs/PARTITION_PERFORMANCE_VALIDATION_MATRIX.md",
        "cube_web/docs/PARTITION_GRID_METHOD_AND_HISTORY.md",
    }
)
HISTORICAL_LABEL = "> Historical record — retained as dated evidence only; it is not the current production contract."


@dataclass(frozen=True)
class Inventory:
    paths: tuple[str, ...]
    excluded_files: tuple[str, ...]


@dataclass(frozen=True)
class ScanRule:
    name: str
    patterns: tuple[str, ...]
    allowlist: tuple[str, ...] = ()


class ScanResult(dict[str, object]):
    """JSON-serializable scanner result with stable mapping access."""


def _legacy_grid_match(content: str) -> Iterable[str]:
    patterns = (
        r"(?:grid_type|grid)\s*[:=]\s*[\"']s2[\"']",
        r"(?:grid_type|grid)\s*[:=]\s*[\"']tile_matrix[\"']",
        r"(?:grid_type|grid)\s*[:=]\s*[\"']plane_grid[\"']",
        r"value=[\"'](?:s2|tile_matrix|plane_grid)[\"']",
        r"`(?:s2|tile_matrix|plane_grid)`",
    )
    for pattern in patterns:
        yield from re.findall(pattern, content, flags=re.IGNORECASE)


def _request_level_match(path: str, content: str) -> Iterable[str]:
    for match in re.finditer(r"\bgrid_level_mode\b", content):
        before = content[max(0, match.start() - 200):match.start()]
        if "forbiddenRequestFields" not in before:
            yield match.group(0)
    if not (path.startswith("cube_web/frontend/src/") or path in CURRENT_DOC_PATHS):
        return
    for match in re.finditer(r"[\"']grid_level[\"']\s*:", content):
        before = content[max(0, match.start() - 200):match.start()].lower()
        if re.search(r"(?:request|payload|body|json|strictpartitionrequest)", before):
            yield match.group(0)


def _isea_dependency_match(path: str, content: str) -> Iterable[str]:
    lower_path = path.lower()
    lower_content = content.lower()
    if any(token in lower_path for token in ("pyproject", "requirements", "package", "lock")):
        yield from re.findall(
            r"(?i)(?:(?:[\"'](?:h3|dggrid)[^\"']*[\"'])|(?:^\s*(?:h3|dggrid)\s*[<=>]))",
            content,
            flags=re.MULTILINE,
        )
        return
    if "isea" in lower_path:
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.lower() in {"h3", "dggrid"}:
                        yield alias.name
            elif isinstance(node, ast.ImportFrom) and (node.module or "").lower() in {"h3", "dggrid"}:
                yield node.module or ""
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id.lower() in {"h3", "dggrid"}:
                yield f"{node.value.id}.{node.attr}"
        return
    if "docs/" in lower_path and "isea" in lower_content:
        yield from re.findall(
            r"(?i)\bisea4h\b[^.\n]{0,100}\b(?:uses?|using|backed\s+by|via)\s+(?:h3|dggrid)\b",
            content,
        )


def _forbidden_published_match(content: str) -> Iterable[str]:
    yield from re.findall(r"(?<!un)\bpublished\b", content, flags=re.IGNORECASE)


DEFAULT_RULES = (
    ScanRule("legacy_grid", ()),
    ScanRule("cog_conversion", (r"\b(?:convert_asset_to_cog|cog_workers|cog_overwrite)\b", r"\b(?:reproject(?:ion)?|source[_ ]upload)\b")),
    ScanRule("legacy_request_level", ()),
    ScanRule("isea_h3_runtime_dependency", ()),
    ScanRule("forbidden_published_status", ()),
)
CANONICAL_ALLOWLIST_PATTERNS = {
    "legacy_grid": (r"\b(?:s2|tile_matrix|plane_grid)\b",),
    "cog_conversion": (r"\b(?:convert_asset_to_cog|cog_workers|cog_overwrite|reproject(?:ion)?)\b",),
    "legacy_request_level": (r"\b(?:grid_level|grid_level_mode)\b",),
    "isea_h3_runtime_dependency": (r"(?i)\b(?:h3|dggrid)\b",),
}


def _normalize_path(path: str) -> str:
    candidate = PurePosixPath(path.replace("\\", "/"))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"inventory path must be repository-relative: {path}")
    return candidate.as_posix()


def _is_text_path(path: str) -> bool:
    return PurePosixPath(path).suffix.lower() in TEXT_SUFFIXES


def _is_excluded(path: str) -> bool:
    return path in TOOL_TEST_EXCLUSIONS or path.startswith(EXCLUDED_PREFIXES) or path.startswith(TOOL_OUTPUT_PREFIXES)


def build_inventory(root: Path, *, paths: Iterable[str] | None = None) -> Inventory:
    """Build an ordered text-file inventory from Git, with testable path injection."""
    if paths is None:
        result = subprocess.run(
            ["git", "ls-files", "-co", "--exclude-standard", "-z"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        paths = (item.decode("utf-8") for item in result.stdout.split(b"\0") if item)

    active: list[str] = []
    excluded: list[str] = []
    for raw_path in paths:
        path = _normalize_path(raw_path)
        if not _is_text_path(path):
            continue
        if _is_excluded(path):
            excluded.append(path)
        else:
            active.append(path)
    return Inventory(paths=tuple(sorted(set(active))), excluded_files=tuple(sorted(set(excluded))))


def _is_production_or_current_doc(path: str) -> bool:
    return "/tests/" not in f"/{path}" and not path.startswith("tests/")


def _asserts_rejection(path: str, content: str, rule: str, token: str) -> bool:
    if not re.search(r"\b(?:assert|raises|reject|forbidden|invalid|not\s+in)\b", content, flags=re.IGNORECASE):
        return False
    canonical_patterns = CANONICAL_ALLOWLIST_PATTERNS.get(rule, ())
    if not any(re.search(pattern, token, flags=re.IGNORECASE) for pattern in canonical_patterns):
        return False
    return bool(re.search(re.escape(token), content, flags=re.IGNORECASE))


def _rule_matches(rule: ScanRule, path: str, content: str) -> list[str]:
    if rule.name == "legacy_grid":
        return list(_legacy_grid_match(content)) + [
            match for pattern in rule.patterns for match in re.findall(pattern, content, flags=re.IGNORECASE)
        ]
    if rule.name == "legacy_request_level":
        return list(_request_level_match(path, content))
    if rule.name == "isea_h3_runtime_dependency":
        return list(_isea_dependency_match(path, content))
    if rule.name == "forbidden_published_status":
        return list(_forbidden_published_match(content))
    return [match for pattern in rule.patterns for match in re.findall(pattern, content, flags=re.IGNORECASE)]


def _error(rule: str, path: str, message: str, token: str | None = None) -> dict[str, str]:
    error = {"rule": rule, "path": path, "message": message}
    if token is not None:
        error["token"] = token
    return error


CURRENT_DOC_PATHS = frozenset(
    {
        "docs/PRODUCTION_TEST_ACCEPTANCE.md",
        "cube_split/docs/WORKFLOW.md",
        "cube_encoder/docs/ARCHITECTURE.md",
        "cube_encoder/docs/README.md",
    }
)


def _rule_applies(rule: ScanRule, path: str) -> bool:
    """Limit textual rules to the M4/M5 active contracts they govern."""
    is_test = path.startswith("tests/") or "/tests/" in f"/{path}"
    if is_test:
        return path in rule.allowlist
    production = path.startswith(("cube_encoder/grid_core/", "cube_web/cube_web/")) or (
        path.startswith("cube_split/cube_split/") and not path.startswith("cube_split/cube_split/scripts/")
    )
    active_contract = production or path.startswith("cube_web/frontend/src/") or path in CURRENT_DOC_PATHS
    if rule.name in {"legacy_grid", "cog_conversion", "legacy_request_level"}:
        return active_contract
    if rule.name == "isea_h3_runtime_dependency":
        return path.startswith("cube_encoder/grid_core/") or path in CURRENT_DOC_PATHS or path.endswith("/docs/current.md") or any(
            token in path.lower() for token in ("pyproject", "requirements", "package", "lock")
        )
    if rule.name == "forbidden_published_status":
        return path.startswith(("cube_web/frontend/src/", "cube_web/cube_web/")) or path in CURRENT_DOC_PATHS
    return False


def scan_repository(root: Path, inventory: Inventory, rules: tuple[ScanRule, ...]) -> ScanResult:
    """Scan a fixed inventory and return a deterministic JSON-serializable result."""
    unknown_rules = {rule.name for rule in rules} - RULE_NAMES
    if unknown_rules:
        raise ValueError(f"unknown rules: {sorted(unknown_rules)}")
    duplicate_rules = [rule.name for rule in rules if sum(other.name == rule.name for other in rules) > 1]
    if duplicate_rules:
        raise ValueError(f"duplicate rules: {sorted(set(duplicate_rules))}")

    normalized_paths = {_normalize_path(path) for path in inventory.paths}
    paths = tuple(sorted(path for path in normalized_paths if not _is_excluded(path)))
    excluded = tuple(
        sorted({_normalize_path(path) for path in inventory.excluded_files} | (normalized_paths - set(paths)))
    )
    errors: list[dict[str, str]] = []
    allowlists: dict[str, list[str]] = {}
    content_by_path: dict[str, str] = {}

    for path in paths:
        file_path = root / path
        if not file_path.is_file():
            errors.append(_error("inventory", path, "inventoried file does not exist"))
            continue
        content_by_path[path] = file_path.read_text(encoding="utf-8", errors="replace")

    for path in paths:
        if path in HISTORICAL_PATHS:
            if HISTORICAL_LABEL not in content_by_path.get(path, ""):
                errors.append(_error("historical_label", path, "historical record is missing the exact non-normative label"))

    for rule in rules:
        if rule.name == "forbidden_published_status" and rule.allowlist:
            errors.append(_error(rule.name, "<allowlist>", "forbidden_published_status has no allowlist"))
            continue

        validated_allowlist: list[str] = []
        for allowlisted_path in rule.allowlist:
            path = _normalize_path(allowlisted_path)
            content = content_by_path.get(path)
            if path not in paths:
                errors.append(_error(rule.name, path, "allowlisted path is not present in inventory"))
            elif _is_production_or_current_doc(path):
                errors.append(_error(rule.name, path, "production/current-documentation path cannot be allowlisted"))
            elif content is None or not any(_asserts_rejection(path, content, rule.name, match) for match in _rule_matches(rule, path, content)):
                errors.append(_error(rule.name, path, "allowlisted path does not assert rejection for the exact rule token"))
            else:
                validated_allowlist.append(path)
        if validated_allowlist:
            allowlists[rule.name] = validated_allowlist

        for path, content in content_by_path.items():
            if path in HISTORICAL_PATHS or path in rule.allowlist or not _rule_applies(rule, path):
                continue
            for token in _rule_matches(rule, path, content):
                errors.append(_error(rule.name, path, "forbidden active-tree token", token))

    encoded_inventory = "\n".join(paths).encode("utf-8")
    rule_results = {
        rule.name: {
            "status": "FAIL" if any(error["rule"] == rule.name for error in errors) else "PASS",
            "error_count": sum(error["rule"] == rule.name for error in errors),
        }
        for rule in rules
    }
    return ScanResult({
        "status": "FAIL" if errors else "PASS",
        "scanned_file_count": len(paths),
        "scanned_paths": list(paths),
        "excluded_files": list(excluded),
        "inventory_digest": hashlib.sha256(encoded_inventory).hexdigest(),
        "rules": rule_results,
        "allowlists": allowlists,
        "errors": errors,
    })


def _write_json_atomically(path: Path, payload: ScanResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    temporary_path.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    scan = scan_repository(root, build_inventory(root), DEFAULT_RULES)
    output_path = args.json_out if args.json_out.is_absolute() else root / args.json_out
    _write_json_atomically(output_path, scan)
    print(f"M5_SCAN_STATUS={scan['status']}")
    return 0 if scan["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
