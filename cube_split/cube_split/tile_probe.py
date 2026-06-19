from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib import request

TRACE_URL = "http://10.3.100.182:6000/api/v1/traces"
TRACE_TIMEOUT_SEC = 2.0
TRACE_BATCH_SIZE = 200


@dataclass(frozen=True)
class TileProbeMetric:
    task_name: str
    tile_type: str
    method_name: str
    attributes: Mapping[str, Any] | None = None


def report_tile_metric(task_name: str, tile_type: str, method_name: str = "") -> None:
    report_tile_metrics((TileProbeMetric(task_name, tile_type, method_name),))


def report_tile_metrics(metrics: Iterable[TileProbeMetric]) -> None:
    batch: list[dict[str, object]] = []
    for metric in metrics:
        batch.append(_build_span(metric))
        if len(batch) >= TRACE_BATCH_SIZE:
            _post_spans(batch)
            batch = []
    if batch:
        _post_spans(batch)


def _build_span(metric: TileProbeMetric) -> dict[str, object]:
    attributes = [
        {
            "key": "tile.type",
            "value": {"stringValue": metric.tile_type},
        },
        {
            "key": "rpc.method",
            "value": {"stringValue": metric.method_name},
        },
    ]
    for key, value in (metric.attributes or {}).items():
        attr = _build_attribute(str(key), value)
        if attr is not None:
            attributes.append(attr)
    return {
        "name": metric.task_name,
        "attributes": attributes,
    }


def _build_attribute(key: str, value: Any) -> dict[str, object] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        encoded: dict[str, object] = {"boolValue": value}
    elif isinstance(value, int):
        encoded = {"intValue": value}
    elif isinstance(value, float):
        encoded = {"doubleValue": value}
    else:
        encoded = {"stringValue": str(value)}
    return {"key": key, "value": encoded}


def _post_spans(spans: list[dict[str, object]]) -> None:
    payload = {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "spans": spans,
                    }
                ]
            }
        ]
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(TRACE_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=TRACE_TIMEOUT_SEC):
            return
    except Exception as exc:
        print("连不上监控系统，但不影响正常业务运行", exc)
