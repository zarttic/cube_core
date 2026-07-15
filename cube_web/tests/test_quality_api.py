from cube_web.routes.quality import create_quality_router


def test_only_normalized_quality_routes_exist() -> None:
    paths = {route.path for route in create_quality_router().routes}
    assert paths == {
        "/quality/records",
        "/quality/records/{quality_run_id}",
        "/quality/records/{quality_run_id}/results",
        "/quality/records/{quality_run_id}/errors",
        "/quality/records/{quality_run_id}/errors/export",
        "/quality/runs",
    }
    assert not any(segment in path for path in paths for segment in ("/history", "/latest", "/report", "/optical", "/radar", "/product", "/carbon"))
