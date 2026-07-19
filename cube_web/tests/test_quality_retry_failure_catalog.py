from __future__ import annotations

import ast
import json
import re
from pathlib import Path

from cube_web.services.quality_rules import default_rule_registry


ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = ROOT / "docs" / "quality_retry_failure_matrix.json"
RULES_PATH = ROOT / "cube_web" / "cube_web" / "services" / "quality_rules.py"
LABELS_PATH = ROOT / "cube_web" / "frontend" / "src" / "utils" / "qualityLabels.js"
OPEN_ISSUES_PATH = ROOT / "docs" / "OPEN_ISSUES.md"


def _catalog() -> dict:
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def _static_finding_codes() -> set[str]:
    tree = ast.parse(RULES_PATH.read_text(encoding="utf-8"))
    codes = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "QualityFinding":
            continue
        if not node.args:
            continue
        first_arg = node.args[0]
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            codes.add(first_arg.value)
            continue
        # Loader-declared findings preserve their supplied code and use a
        # literal fallback. Include that fallback while the wildcard catalog
        # entry accounts for arbitrary loader-owned codes.
        for child in ast.walk(first_arg):
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                if child.value.endswith("_defect"):
                    codes.add(child.value)
    return codes


def test_catalog_covers_every_quality_rule_and_static_finding_code() -> None:
    catalog = _catalog()
    findings = catalog["quality_findings"]
    by_rule = {item["rule_code"]: item for item in findings}
    registry = {rule.code: rule for rule in default_rule_registry().all()}

    assert set(by_rule) == set(registry)
    assert all(item["mandatory"] is registry[item["rule_code"]].mandatory for item in findings)
    catalog_codes = {
        code
        for item in findings
        for code in item["trigger_codes"]
        if not code.endswith(":*")
    }
    assert _static_finding_codes() == catalog_codes

    labels_source = LABELS_PATH.read_text(encoding="utf-8")
    error_labels = labels_source.split("const errorLabels = {", 1)[1].split("};", 1)[0]
    labelled_codes = set(re.findall(r"(?:^|,)\s*([a-z][a-z0-9_]*)\s*:", error_labels))
    assert _static_finding_codes() <= labelled_codes


def test_catalog_covers_terminal_and_retry_state_space_with_actionable_evidence() -> None:
    catalog = _catalog()
    assert {item["status"] for item in catalog["quality_terminal"]} == {"pass", "warn", "fail", "error"}
    assert {item["id"] for item in catalog["quality_retry"]} == {
        "quality-no-current-output", "quality-current-output-changed", "quality-historical-output",
        "quality-duplicate-trigger", "quality-stale-lease", "quality-rule-exception",
        "quality-empty-export", "quality-warn-gate",
    }
    assert len(catalog["partition_retry"]) >= 8
    assert len(catalog["ingest_retry"]) >= 8
    assert {item["id"] for item in catalog["open_issues"]} == {
        "issue-partial-repartition-current-output",
        "issue-retry-current-attempt",
    }

    all_items = [
        *catalog["quality_terminal"],
        *catalog["quality_retry"],
        *catalog["quality_findings"],
        *catalog["partition_retry"],
        *catalog["ingest_retry"],
        *catalog["open_issues"],
    ]
    identifiers = [item["id"] for item in all_items]
    assert len(identifiers) == len(set(identifiers))
    for item in all_items:
        assert item["current_behavior"].strip()
        assert item["problem"].strip()
        assert item["solution"].strip()
        evidence_path = item["evidence"].split("::", 1)[0]
        resolved_evidence = ROOT / evidence_path
        assert resolved_evidence.exists(), item["id"]
        if resolved_evidence.suffix == ".py" and "::" in item["evidence"]:
            symbol = item["evidence"].split("::", 1)[1]
            source_tree = ast.parse(resolved_evidence.read_text(encoding="utf-8"))
            symbols = {
                node.name
                for node in ast.walk(source_tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            }
            assert symbol in symbols, item["id"]

    open_issue_headings = set(re.findall(r"^## (.+)$", OPEN_ISSUES_PATH.read_text(encoding="utf-8"), re.MULTILINE))
    catalog_issue_headings = {
        item["evidence"].split("::", 1)[1]
        for item in catalog["open_issues"]
        if "::" in item["evidence"]
    }
    assert catalog_issue_headings == open_issue_headings
