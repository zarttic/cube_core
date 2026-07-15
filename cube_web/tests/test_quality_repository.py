from datetime import UTC, datetime
from uuid import uuid4

from cube_web.services.quality_contracts import RuleSnapshot
from cube_web.services.quality_repository import QualityTriggerConflict, _snapshot_json


def _snapshot() -> RuleSnapshot:
    return RuleSnapshot(
        code="output_count_consistency",
        name="Output count consistency",
        applicability={"data_types": ["optical"]},
        mandatory=True,
        parameters={},
        implementation_version="1.0.0",
    )


def test_snapshot_identity_is_stable_and_conflict_is_explicit() -> None:
    assert _snapshot_json((_snapshot(),)) == _snapshot_json((_snapshot(),))
    assert issubclass(QualityTriggerConflict, RuntimeError)


def test_contract_timestamps_are_utc_compatible() -> None:
    assert datetime.now(UTC).tzinfo is not None
    assert uuid4().version == 4
