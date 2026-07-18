from cube_web.routes.quality import create_quality_router


def test_only_normalized_quality_routes_exist() -> None:
    paths = {route.path for route in create_quality_router().routes}
    assert paths == {
        "/quality/records",
        "/quality/records/{quality_run_id}",
        "/quality/records/{quality_run_id}/results",
        "/quality/records/{quality_run_id}/errors",
        "/quality/records/{quality_run_id}/errors/export",
        "/quality/rules",
        "/quality/runs",
    }
    assert not any(segment in path for path in paths for segment in ("/history", "/latest", "/report", "/optical", "/radar", "/product", "/carbon"))


def test_rule_catalog_exposes_requirement_and_product_applicability() -> None:
    route = next(route for route in create_quality_router().routes if route.path == "/quality/rules")
    body = route.endpoint()
    rules = {item["code"]: item for item in body["items"]}

    assert body["rule_set_version"]
    assert rules["asset_readability"]["mandatory"] is True
    assert rules["metadata_completeness"]["mandatory"] is False
    assert rules["declared_metadata_defects"]["mandatory"] is True
    assert rules["declared_metadata_warnings"]["mandatory"] is False
    assert rules["product_year_consistency"]["applicability"]["data_types"] == ["product"]
    assert rules["carbon_schema"]["applicability"]["data_types"] == ["carbon"]
