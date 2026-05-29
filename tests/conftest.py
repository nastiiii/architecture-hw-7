from __future__ import annotations

import time
from collections.abc import Iterator
from uuid import UUID

import pytest
import requests
from cassandra.cluster import Cluster, Session
from cassandra.policies import WhiteListRoundRobinPolicy


PRODUCER_URL = "http://localhost:8000"
CONSUMER_URL = "http://localhost:8010"


def wait_http_ok(url: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"{url} did not become healthy")


@pytest.fixture(scope="session", autouse=True)
def running_system() -> None:
    wait_http_ok(f"{PRODUCER_URL}/health")
    wait_http_ok(f"{CONSUMER_URL}/health")


@pytest.fixture(scope="session")
def cassandra_session() -> Iterator[Session]:
    cluster = Cluster(
        ["127.0.0.1"],
        protocol_version=5,
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
    )
    session = cluster.connect("warehouse")
    try:
        yield session
    finally:
        cluster.shutdown()


def delete_test_product(session: Session, product_id: str, zone_id: str, event_ids: list[str], order_id: str | None = None) -> None:
    session.execute("DELETE FROM inventory_by_product_zone WHERE product_id = %s AND zone_id = %s", (product_id, zone_id))
    session.execute("DELETE FROM inventory_by_product WHERE product_id = %s", (product_id,))
    session.execute("DELETE FROM inventory_by_zone WHERE zone_id = %s AND product_id = %s", (zone_id, product_id))
    for event_id in event_ids:
        session.execute("DELETE FROM processed_events WHERE event_id = %s", (UUID(event_id),))
    if order_id:
        session.execute("DELETE FROM orders_by_id WHERE order_id = %s", (order_id,))
