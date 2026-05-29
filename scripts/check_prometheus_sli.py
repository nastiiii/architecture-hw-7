from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

import requests


PROMETHEUS_URL = "http://localhost:9090"
ARTIFACT_DIR = Path("artifacts")
ARTIFACT_DIR.mkdir(exist_ok=True)

CHECKS = [
    {
        "name": "producer_error_rate",
        "sli": "API availability errors",
        "query": """
            sum(rate(http_request_errors_total{job="producer"}[1m]))
            /
            clamp_min(sum(rate(http_requests_total{job="producer"}[1m])), 0.001)
        """,
        "operator": "<",
        "threshold": 0.01,
        "slo": "< 1% errors",
        "failure_threshold": ">= 5% errors",
    },
    {
        "name": "producer_p95_latency",
        "sli": "End-to-end API latency",
        "query": """
            histogram_quantile(
              0.95,
              sum by (le) (http_request_duration_seconds_bucket{job="producer",endpoint="/events"})
            )
        """,
        "operator": "<",
        "threshold": 0.5,
        "slo": "p95 < 500ms",
        "failure_threshold": "p95 > 1000ms",
    },
    {
        "name": "consumer_lag",
        "sli": "Event processing delay proxy",
        "query": "sum(consumer_lag)",
        "operator": "<",
        "threshold": 2000,
        "slo": "consumer lag < 100 messages",
        "failure_threshold": "consumer lag > 2000 messages under CI load",
    },
]


def prometheus_query(query: str) -> float:
    response = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": " ".join(query.split())},
        timeout=10,
    )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    result = payload["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def passes(value: float, operator: str, threshold: float) -> bool:
    if operator == "<":
        return value < threshold
    if operator == ">":
        return value > threshold
    raise ValueError(f"unsupported operator: {operator}")


def main() -> int:
    time.sleep(10)
    results = []
    failed = False
    for check in CHECKS:
        value = prometheus_query(check["query"])
        ok = passes(value, check["operator"], check["threshold"])
        failed = failed or not ok
        results.append({**check, "value": value, "ok": ok})
        status = "OK" if ok else "FAIL"
        print(f"{status} {check['name']}: value={value:.6f} expected {check['operator']} {check['threshold']}")

    (ARTIFACT_DIR / "prometheus-sli-results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
