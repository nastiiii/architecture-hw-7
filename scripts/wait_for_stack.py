from __future__ import annotations

import sys
import time

import requests


TARGETS = {
    "producer": "http://localhost:8000/health",
    "consumer": "http://localhost:8010/health",
    "prometheus": "http://localhost:9090/-/ready",
    "grafana": "http://localhost:3000/api/health",
    "alertmanager": "http://localhost:9093/-/ready",
}


def wait_target(name: str, url: str, timeout_seconds: int = 360) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code < 500:
                print(f"{name} is ready: {response.status_code}")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise TimeoutError(f"{name} did not become ready at {url}")


def main() -> int:
    for name, url in TARGETS.items():
        wait_target(name, url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
