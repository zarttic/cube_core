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
    return _partition_runners()._run_optical_partition_retry(payload)


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
    raise HTTPException(status_code=501, detail="Radar partition demo is not implemented")


def partition_radar_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    raise HTTPException(status_code=501, detail="Radar partition retry is not implemented")


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
