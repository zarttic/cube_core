from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cube_web.services.quality_contracts import (
    MAX_PAGE_SIZE,
    Page,
    Publication,
    QualityErrorFilter,
    RuleSnapshot,
    page_offset,
    validate_sort,
)


def test_rule_snapshot_is_complete_and_frozen() -> None:
    snapshot = RuleSnapshot(
        code="output_count_consistency",
        name="Output count consistency",
        applicability={"data_types": ["optical", "radar", "product", "carbon"]},
        mandatory=True,
        parameters={"batch_size": 1000},
        implementation_version="1.0.0",
    )

    assert snapshot.model_dump(mode="json")["parameters"] == {"batch_size": 1000}
    with pytest.raises(ValidationError):
        snapshot.code = "changed"
    with pytest.raises(ValidationError):
        RuleSnapshot.model_validate({**snapshot.model_dump(), "unexpected": True})


def test_filter_sort_and_page_contracts_are_closed() -> None:
    assert QualityErrorFilter().active() is False
    assert QualityErrorFilter(rule_code="bounds").active() is True
    assert page_offset(2, 20) == 20
    assert validate_sort("completed_at", "desc", {"completed_at", "quality_run_id"}) == ("completed_at", "desc")
    with pytest.raises(ValueError, match="sort_by"):
        validate_sort("raw_sql", "desc", {"completed_at"})
    with pytest.raises(ValueError, match="sort_order"):
        validate_sort("completed_at", "descending", {"completed_at"})
    with pytest.raises(ValueError, match="page"):
        page_offset(0, 20)
    with pytest.raises(ValueError, match="page"):
        page_offset(1, MAX_PAGE_SIZE + 1)


def test_page_is_immutable_and_publication_forbids_published() -> None:
    page = Page[str](items=("one",), total=1, page=1, page_size=20)
    assert page.model_dump() == {"items": ("one",), "total": 1, "page": 1, "page_size": 20}
    with pytest.raises(ValidationError):
        page.total = 2

    with pytest.raises(ValidationError):
        Publication(
            publication_id=uuid4(),
            dataset_id="dataset-a",
            output_version="output-a",
            quality_run_id=uuid4(),
            status="published",
            service_version_id=None,
            requested_by="alice",
            requested_at=datetime.now(timezone.utc),
            activated_at=None,
            failure=None,
            withdrawn_by=None,
            withdrawn_at=None,
            withdrawal_reason=None,
        )
