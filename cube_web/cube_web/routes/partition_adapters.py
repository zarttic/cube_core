from __future__ import annotations

from importlib import import_module

from fastapi import HTTPException

from cube_web.schemas import PartitionDemoRequest, PartitionRetryRequest, payload_from_model


def _partition_runners():
    return import_module("cube_web.services.partition_runners")


def raise_http_unless_cancelled(exc: Exception) -> None:
    if exc.__class__.__name__ == "PartitionCancelledError":
        raise exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


def run_carbon_partition_demo(payload: dict | None = None) -> dict:
    return _partition_runners()._run_carbon_partition_demo(payload=payload)


def run_carbon_partition_test(payload: dict | None = None) -> dict:
    return _partition_runners()._run_carbon_partition_test(payload)


def run_carbon_partition_retry(payload: dict | None = None) -> dict:
    return _partition_runners()._run_carbon_partition_retry(payload)


def run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return _partition_runners()._run_product_partition_demo(payload, mode=mode)


def run_product_partition_test(payload: dict | None = None) -> dict:
    return _partition_runners()._run_product_partition_test(payload)


def run_product_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = run_product_partition_demo(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def run_radar_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return _partition_runners()._run_radar_partition_demo(payload, mode=mode)


def run_radar_partition_test(payload: dict | None = None) -> dict:
    return _partition_runners()._run_radar_partition_test(payload)


def run_radar_partition_retry(payload: dict | None = None) -> dict:
    return _partition_runners()._run_radar_partition_retry(payload)


def run_entity_partition_demo(payload: dict | None = None) -> dict:
    return _partition_runners()._run_entity_partition_demo(payload)


def run_entity_partition_test(payload: dict | None = None) -> dict:
    return _partition_runners()._run_entity_partition_test(payload)


def run_entity_partition_retry(payload: dict | None = None) -> dict:
    return _partition_runners()._run_entity_partition_retry(payload)


def run_optical_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return _partition_runners()._run_optical_partition_from_payload(payload, mode=mode)


def run_optical_partition_demo(payload: dict | None = None) -> dict:
    return _partition_runners()._run_optical_partition_demo(payload)


def run_optical_partition_test(payload: dict | None = None) -> dict:
    return _partition_runners()._run_optical_partition_test(payload)


def run_optical_partition_retry(payload: dict | None = None) -> dict:
    payload = payload or {}
    request = payload.get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    last_result = payload.get("last_result") or {}
    if not isinstance(last_result, dict):
        last_result = {}

    runners = _partition_runners()
    warn_checks = runners._warn_checks_from_result(last_result)
    warning_paths = runners._warning_asset_paths(warn_checks)
    retry_payload, retried_asset_count = runners._retry_payload_for_warning_assets(request_payload, warning_paths)
    result = run_optical_partition_from_payload(retry_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "warning_assets" if retried_asset_count else "full_request",
        "warning_check_names": [str(check.get("name")) for check in warn_checks],
        "warning_asset_count": len(warning_paths),
        "retried_asset_count": retried_asset_count,
    }
    return result


def partition_carbon_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        if payload is None:
            return run_carbon_partition_demo()
        return run_carbon_partition_demo(payload_from_model(payload))
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_carbon_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_carbon_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_carbon_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return run_carbon_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_product_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_product_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_product_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_product_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_product_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return run_product_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_entity_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_entity_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_entity_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_entity_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_entity_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return run_entity_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_radar_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_radar_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_radar_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_radar_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_radar_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return run_radar_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_optical_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_optical_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_optical_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return run_optical_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_optical_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return run_optical_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise_http_unless_cancelled(exc)


def partition_optical_run(payload: PartitionDemoRequest | dict | None = None) -> dict:
    return partition_optical_demo(payload)


def partition_carbon_run(payload: PartitionDemoRequest | dict | None = None) -> dict:
    return partition_carbon_demo(payload)


def partition_product_run(payload: PartitionDemoRequest | dict | None = None) -> dict:
    return partition_product_demo(payload)


def partition_radar_run(payload: PartitionDemoRequest | dict | None = None) -> dict:
    return partition_radar_demo(payload)


def partition_entity_run(payload: PartitionDemoRequest | dict | None = None) -> dict:
    return partition_entity_demo(payload)
