from grid_core.app.perf_smoke import run_perf_smoke


def test_perf_smoke_returns_structured_metrics_without_enforcement():
    report = run_perf_smoke(enforce=False)
    assert "geohash_locate" in report
    assert "mgrs_cover_intersect" in report
    assert "topology_batch_geometries_20" in report
    for row in report.values():
        assert row["avg_ms"] >= 0
        assert row["max_ms"] >= 0
        assert row["threshold_ms"] > 0
