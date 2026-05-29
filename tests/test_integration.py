from __future__ import annotations

import time
from uuid import uuid4

import requests

from conftest import PRODUCER_URL, delete_test_product


def wait_inventory(session, product_id: str, zone_id: str, expected_available: int, expected_reserved: int) -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        row = session.execute(
            """
            SELECT available_quantity, reserved_quantity
            FROM inventory_by_product_zone
            WHERE product_id = %s AND zone_id = %s
            """,
            (product_id, zone_id),
        ).one()
        if row and row.available_quantity == expected_available and row.reserved_quantity == expected_reserved:
            return
        time.sleep(1)
    raise AssertionError(f"inventory did not reach available={expected_available}, reserved={expected_reserved}")


def test_producer_consumer_cassandra_integration(cassandra_session) -> None:
    product_id = f"SKU-IT-{uuid4().hex[:8]}"
    zone_id = "ZONE-IT"
    received_id = str(uuid4())
    reserved_id = str(uuid4())
    try:
        response = requests.post(
            f"{PRODUCER_URL}/events",
            json={
                "event_id": received_id,
                "event_type": "PRODUCT_RECEIVED",
                "product_id": product_id,
                "zone_id": zone_id,
                "quantity": 25,
                "timestamp": "2026-05-29T09:00:00Z",
            },
            timeout=10,
        )
        assert response.status_code == 200
        assert response.json()["event_id"] == received_id
        wait_inventory(cassandra_session, product_id, zone_id, 25, 0)

        response = requests.post(
            f"{PRODUCER_URL}/events",
            json={
                "event_id": reserved_id,
                "event_type": "PRODUCT_RESERVED",
                "product_id": product_id,
                "zone_id": zone_id,
                "quantity": 7,
                "timestamp": "2026-05-29T09:01:00Z",
            },
            timeout=10,
        )
        assert response.status_code == 200
        wait_inventory(cassandra_session, product_id, zone_id, 18, 7)
    finally:
        delete_test_product(cassandra_session, product_id, zone_id, [received_id, reserved_id])
