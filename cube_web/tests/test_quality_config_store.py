from cube_web.services.config_store import normalized_stored_config
from cube_web.services.quality_rules import default_enabled_optional_rules


def test_legacy_quality_config_defaults_to_every_optional_rule() -> None:
    config = normalized_stored_config({"quality": {"optical": {"target_crs": "EPSG:4326", "history_limit": 20}}})

    assert set(config["quality"]["enabled_optional_rules"]) == set(default_enabled_optional_rules())
