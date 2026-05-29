from __future__ import annotations

import time
from uuid import uuid4

import requests

from conftest import PRODUCER_URL, delete_test_product


def wait_order(session, order_id: str, expected_status: str) -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        row = session.execute("SELECT status FROM orders_by_id WHERE order_id = %s", (order_id,)).one()
        if row and row.status == expected_status:
            return
        time.sleep(1)
    raise AssertionError(f"order {order_id} did not reach status={expected_status}")


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
    raise AssertionError("inventory state was not updated")


def test_create_and_complete_order_e2e(cassandra_session) -> None:
    product_id = f"SKU-E2E-{uuid4().hex[:8]}"
    zone_id = "ZONE-E2E"
    order_id = f"ORDER-E2E-{uuid4().hex[:8]}"
    received_id = str(uuid4())
    order_created_id = str(uuid4())
    order_completed_id = str(uuid4())
    try:
        response = requests.post(
            f"{PRODUCER_URL}/events",
            json={
                "event_id": received_id,
                "event_type": "PRODUCT_RECEIVED",
                "product_id": product_id,
                "zone_id": zone_id,
                "quantity": 20,
                "timestamp": "2026-05-29T10:00:00Z",
            },
            timeout=10,
        )
        assert response.status_code == 200
        assert response.json() == {"event_id": received_id}
        wait_inventory(cassandra_session, product_id, zone_id, 20, 0)

        response = requests.post(
            f"{PRODUCER_URL}/events",
            json={
                "event_id": order_created_id,
                "event_type": "ORDER_CREATED",
                "order_id": order_id,
                "items": [{"product_id": product_id, "zone_id": zone_id, "quantity": 6}],
                "timestamp": "2026-05-29T10:01:00Z",
            },
            timeout=10,
        )
        assert response.status_code == 200
        wait_order(cassandra_session, order_id, "CREATED")
        wait_inventory(cassandra_session, product_id, zone_id, 14, 6)

        response = requests.post(
            f"{PRODUCER_URL}/events",
            json={
                "event_id": order_completed_id,
                "event_type": "ORDER_COMPLETED",
                "order_id": order_id,
                "timestamp": "2026-05-29T10:02:00Z",
            },
            timeout=10,
        )
        assert response.status_code == 200
        wait_order(cassandra_session, order_id, "COMPLETED")
        wait_inventory(cassandra_session, product_id, zone_id, 14, 0)
    finally:
        delete_test_product(
            cassandra_session,
            product_id,
            zone_id,
            [received_id, order_created_id, order_completed_id],
            order_id=order_id,
        )
