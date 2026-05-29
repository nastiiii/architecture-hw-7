from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import WarehouseEventIn


def test_product_received_requires_positive_quantity() -> None:
    event = WarehouseEventIn(
        event_type="PRODUCT_RECEIVED",
        product_id="SKU-UNIT",
        zone_id="ZONE-A",
        quantity=5,
        timestamp=datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc),
    )

    assert event.product_id == "SKU-UNIT"
    assert event.quantity == 5


def test_product_received_rejects_missing_zone() -> None:
    with pytest.raises(ValidationError):
        WarehouseEventIn(
            event_type="PRODUCT_RECEIVED",
            product_id="SKU-UNIT",
            quantity=5,
            timestamp=datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc),
        )


def test_order_created_requires_items() -> None:
    with pytest.raises(ValidationError):
        WarehouseEventIn(
            event_type="ORDER_CREATED",
            order_id="ORDER-UNIT",
            timestamp=datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc),
        )
