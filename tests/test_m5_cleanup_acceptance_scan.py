"""Regression coverage for the M5 active-tree contract scanner."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.m5_cleanup_acceptance_scan import (
    HISTORICAL_LABEL,
    Inventory,
    ScanRule,
    build_inventory,
    scan_repository,
)


def write_files(root: Path, files: dict[str, str]) -> None:
    for relative_path, content in files.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def inventory_for(root: Path, *paths: str) -> Inventory:
    return Inventory(paths=tuple(sorted(paths)), excluded_files=())


def test_inventory_scans_production_and_current_docs_and_excludes_plans(tmp_path: Path):
    write_files(
        tmp_path,
        {
            "cube_web/cube_web/app.py": "application = object()\n",
            "docs/current.md": "Current contract.\n",
            "docs/superpowers/plans/old.md": "legacy plan\n",
        },
    )

    inventory = build_inventory(tmp_path, paths=(
        "cube_web/cube_web/app.py",
        "docs/current.md",
        "docs/superpowers/plans/old.md",
    ))
    scan = scan_repository(tmp_path, inventory, ())

    assert scan["status"] == "PASS"
    assert scan["scanned_file_count"] == 2
    assert "cube_web/cube_web/app.py" in scan["scanned_paths"]
    assert "docs/current.md" in scan["scanned_paths"]
    assert "docs/superpowers/plans/old.md" in scan["excluded_files"]


def test_contract_rules_apply_to_backend_production_paths(tmp_path: Path):
    paths = {
        "cube_web/cube_web/service.py": 'payload = {"grid_level_mode": "auto"}\n',
        "cube_split/cube_split/partition/job.py": "convert_asset_to_cog(source)\n",
        "cube_encoder/grid_core/app/config.py": 'grid_type = "s2"\n',
    }
    write_files(tmp_path, paths)

    scan = scan_repository(
        tmp_path,
        inventory_for(tmp_path, *paths),
        (
            ScanRule("legacy_grid", ()),
            ScanRule("cog_conversion", (r"\bconvert_asset_to_cog\b",)),
            ScanRule("legacy_request_level", ()),
        ),
    )

    assert scan["status"] == "FAIL"
    assert {error["rule"] for error in scan["errors"]} == {
        "legacy_grid",
        "cog_conversion",
        "legacy_request_level",
    }


def test_allowlist_requires_exact_rule_inventoried_rejection_test_and_token(tmp_path: Path):
    path = "cube_encoder/tests/test_legacy_grid_rejection.py"
    write_files(tmp_path, {path: 'assert "s2" not in supported_grids\n'})
    rule = ScanRule("legacy_grid", (r"\bs2\b",), allowlist=(path,))

    accepted = scan_repository(tmp_path, inventory_for(tmp_path, path), (rule,))
    assert accepted["status"] == "PASS"
    assert accepted["allowlists"] == {"legacy_grid": [path]}

    wrong_rule = ScanRule("cog_conversion", (r"\bs2\b",), allowlist=(path,))
    rejected = scan_repository(tmp_path, inventory_for(tmp_path, path), (wrong_rule,))
    assert rejected["status"] == "FAIL"
    assert "does not assert rejection" in rejected["errors"][0]["message"]

    production_path = "cube_encoder/grid_core/app.py"
    write_files(tmp_path, {production_path: 'assert "s2" not in supported_grids\n'})
    production = scan_repository(
        tmp_path,
        inventory_for(tmp_path, production_path),
        (ScanRule("legacy_grid", (r"\bs2\b",), allowlist=(production_path,)),),
    )
    assert production["status"] == "FAIL"
    assert "production/current-documentation path" in production["errors"][0]["message"]

    missing = scan_repository(
        tmp_path,
        inventory_for(tmp_path, path),
        (ScanRule("legacy_grid", (r"\bs2\b",), allowlist=("cube_encoder/tests/missing.py",)),),
    )
    assert missing["status"] == "FAIL"
    assert "not present in inventory" in missing["errors"][0]["message"]

    no_rejection_path = "cube_encoder/tests/test_no_rejection.py"
    write_files(tmp_path, {no_rejection_path: 'legacy = "s2"\n'})
    no_rejection = scan_repository(
        tmp_path,
        inventory_for(tmp_path, no_rejection_path),
        (ScanRule("legacy_grid", (r"\bs2\b",), allowlist=(no_rejection_path,)),),
    )
    assert no_rejection["status"] == "FAIL"
    assert "does not assert rejection" in no_rejection["errors"][0]["message"]

    wrong_token = scan_repository(
        tmp_path,
        inventory_for(tmp_path, path),
        (ScanRule("legacy_grid", (r"\bplane_grid\b",), allowlist=(path,)),),
    )
    assert wrong_token["status"] == "FAIL"
    assert "does not assert rejection" in wrong_token["errors"][0]["message"]


@pytest.mark.parametrize(
    ("path", "content"),
    [
        ("cube_encoder/grid_core/app/engines/isea4h.py", "import h3\n"),
        ("cube_encoder/pyproject.toml", 'dependencies = ["h3>=4"]\n'),
        ("cube_split/requirements.txt", "h3==4.1.0\n"),
        ("cube_web/docs/current.md", "ISEA4H uses H3 at runtime.\n"),
    ],
)
def test_isea_h3_runtime_dependency_rejects_runtime_dependencies(tmp_path: Path, path: str, content: str):
    write_files(tmp_path, {path: content})
    rule = ScanRule("isea_h3_runtime_dependency", (r"(?i)\b(?:h3|dggrid)\b",))

    scan = scan_repository(tmp_path, inventory_for(tmp_path, path), (rule,))

    assert scan["status"] == "FAIL"
    assert scan["errors"][0]["rule"] == "isea_h3_runtime_dependency"
    assert scan["errors"][0]["path"] == path


def test_isea_dependency_rule_allows_negative_docs_and_offline_vector_comments(tmp_path: Path):
    paths = {
        "cube_encoder/grid_core/app/engines/isea4h.py": "# DGGRID vector baseline only\n",
        "cube_web/docs/current.md": "ISEA4H has no H3 or DGGRID runtime dependency.\n",
    }
    write_files(tmp_path, paths)
    rule = ScanRule("isea_h3_runtime_dependency", ())

    scan = scan_repository(tmp_path, inventory_for(tmp_path, *paths), (rule,))

    assert scan["status"] == "PASS"


def test_scanner_and_its_test_are_reported_as_tool_exclusions(tmp_path: Path):
    paths = (
        "scripts/m5_cleanup_acceptance_scan.py",
        "tests/test_m5_cleanup_acceptance_scan.py",
    )
    write_files(tmp_path, {path: "import h3\n" for path in paths})
    rule = ScanRule("isea_h3_runtime_dependency", (r"(?i)\bh3\b",))

    scan = scan_repository(tmp_path, inventory_for(tmp_path, *paths), (rule,))

    assert scan["status"] == "PASS"
    assert scan["excluded_files"] == list(paths)


def test_historical_file_requires_exact_label_but_exempts_its_body(tmp_path: Path):
    historical_path = "cube_split/docs/ENTITY_PARTITION_PERFORMANCE.md"
    write_files(tmp_path, {historical_path: f"# Report\n\n{HISTORICAL_LABEL}\n\nlegacy s2\n"})
    scan = scan_repository(
        tmp_path,
        inventory_for(tmp_path, historical_path),
        (ScanRule("legacy_grid", (r"\bs2\b",)),),
    )
    assert scan["status"] == "PASS"

    write_files(tmp_path, {historical_path: "# Report\n\nlegacy s2\n"})
    missing_label = scan_repository(
        tmp_path,
        inventory_for(tmp_path, historical_path),
        (ScanRule("legacy_grid", (r"\bs2\b",)),),
    )
    assert missing_label["status"] == "FAIL"
    assert missing_label["errors"][0]["rule"] == "historical_label"
