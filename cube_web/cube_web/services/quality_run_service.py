from __future__ import annotations

from uuid import uuid4

from cube_web.routes.auth import Actor, require_admin
from cube_web.services.config_store import get_enabled_optional_quality_rules
from cube_web.services.quality_repository import OutputVersionNotFound, allocate_quality_run, lock_dataset, require_open_gauss_domain_store
from cube_web.services.quality_rules import DEFAULT_RULE_SET_VERSION, default_rule_registry, snapshot_rules


def _enabled_optional_rules() -> tuple[str, ...]:
    return get_enabled_optional_quality_rules()


def request_manual_quality_run(dataset_id: str, output_version: str | None, actor: Actor):
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        dataset = lock_dataset(tx, dataset_id)
        selected = output_version or dataset["current_output_version"]
        if selected is None:
            raise OutputVersionNotFound("dataset has no current output")
        if output_version is not None and output_version != dataset["current_output_version"]:
            require_admin(actor)
        snapshot = snapshot_rules(
            default_rule_registry(),
            data_type=str(dataset["data_type"]),
            product_type=dataset.get("product_type"),
            enabled_optional_rules=_enabled_optional_rules(),
        )
        return allocate_quality_run(
            tx,
            dataset_id=dataset_id,
            output_version=str(selected),
            expected_current_output_version=dataset["current_output_version"] if output_version is None else None,
            quality_run_id=uuid4(),
            trigger_event_id=None,
            trigger="manual",
            requested_by=actor.username,
            rule_set_version=DEFAULT_RULE_SET_VERSION,
            rule_snapshot=snapshot,
        )
