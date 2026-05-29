from datetime import datetime, timezone

import pytest

from app.service import WarehouseConsumerService
from app.storage import InventoryState


class FakeStore:
    def __init__(self, state: InventoryState | None = None) -> None:
        self.state = state or InventoryState()

    def read_inventory(self, product_id: str, zone_id: str) -> InventoryState:
        return self.state

    def read_product_zones(self, product_id: str) -> dict[tuple[str, str], InventoryState]:
        return {(product_id, "ZONE-A"): self.state}


def build_service(state: InventoryState | None = None) -> WarehouseConsumerService:
    service = WarehouseConsumerService.__new__(WarehouseConsumerService)
    service.store = FakeStore(state)
    return service


def test_reserve_moves_available_to_reserved() -> None:
    service = build_service(InventoryState(available=10, reserved=2))
    event_ts = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)

    changes = service._reserve("SKU-UNIT", "ZONE-A", 4, event_ts)

    state = changes[("SKU-UNIT", "ZONE-A")]
    assert state.available == 6
    assert state.reserved == 6


def test_reserve_rejects_insufficient_stock() -> None:
    service = build_service(InventoryState(available=1, reserved=0))
    event_ts = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="Insufficient available stock"):
        service._reserve("SKU-UNIT", "ZONE-A", 4, event_ts)


def test_old_event_is_ignored() -> None:
    last_ts = datetime(2026, 5, 29, 10, 0, tzinfo=timezone.utc)
    service = build_service(InventoryState(available=10, reserved=0, last_event_ts=last_ts))

    changes = service._change_available("SKU-UNIT", "ZONE-A", 5, datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc))

    assert changes == {}
