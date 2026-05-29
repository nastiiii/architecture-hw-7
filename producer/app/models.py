from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    product_received = "PRODUCT_RECEIVED"
    product_shipped = "PRODUCT_SHIPPED"
    product_moved = "PRODUCT_MOVED"
    product_reserved = "PRODUCT_RESERVED"
    product_released = "PRODUCT_RELEASED"
    inventory_counted = "INVENTORY_COUNTED"
    order_created = "ORDER_CREATED"
    order_completed = "ORDER_COMPLETED"


class OrderItem(BaseModel):
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class WarehouseEventIn(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    product_id: str | None = None
    zone_id: str | None = None
    from_zone_id: str | None = None
    to_zone_id: str | None = None
    quantity: int | None = None
    counted_quantity: int | None = None
    order_id: str | None = None
    items: list[OrderItem] = Field(default_factory=list)
    supplier_id: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_required_fields(self) -> "WarehouseEventIn":
        single_zone_events = {
            EventType.product_received,
            EventType.product_shipped,
            EventType.product_reserved,
            EventType.product_released,
            EventType.inventory_counted,
        }
        if self.event_type in single_zone_events:
            if not self.product_id or not self.zone_id:
                raise ValueError("product_id and zone_id are required")
        if self.event_type == EventType.product_moved:
            if not self.product_id or not self.from_zone_id or not self.to_zone_id:
                raise ValueError("product_id, from_zone_id and to_zone_id are required")
        if self.event_type == EventType.inventory_counted:
            if self.counted_quantity is None or self.counted_quantity < 0:
                raise ValueError("counted_quantity must be non-negative")
        elif self.event_type not in {EventType.order_created, EventType.order_completed}:
            if self.quantity is None or self.quantity <= 0:
                raise ValueError("quantity must be positive")
        if self.event_type == EventType.order_created:
            if not self.order_id or not self.items:
                raise ValueError("order_id and at least one item are required")
        if self.event_type == EventType.order_completed and not self.order_id:
            raise ValueError("order_id is required")
        return self


class WarehouseEventOut(WarehouseEventIn):
    schema_version: int = 2

    def to_kafka_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["event_id"] = str(self.event_id)
        payload["event_type"] = self.event_type.value
        payload["timestamp"] = self.timestamp.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        payload["items"] = [item.model_dump() for item in self.items]
        payload["schema_version"] = self.schema_version
        return payload
