from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from uuid import uuid4

from .kafka_client import KafkaPublisher
from .models import EventType, WarehouseEventOut


logger = logging.getLogger(__name__)

PRODUCTS = [f"SKU-{idx:03d}" for idx in range(1, 16)]
ZONES = ["ZONE-A", "ZONE-B", "ZONE-C"]


class SyntheticEventGenerator:
    def __init__(self, publisher: KafkaPublisher, interval_seconds: float) -> None:
        self._publisher = publisher
        self._interval_seconds = interval_seconds
        self._enabled = True

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled

    async def run(self) -> None:
        while True:
            if self._enabled:
                self._publish_event()
            await asyncio.sleep(self._interval_seconds)

    def _publish_event(self) -> None:
        product_id = random.choice(PRODUCTS)
        zone_id = random.choice(ZONES)
        event = WarehouseEventOut(
            event_id=uuid4(),
            event_type=random.choice([EventType.product_received, EventType.product_reserved, EventType.product_released]),
            product_id=product_id,
            zone_id=zone_id,
            quantity=random.randint(1, 10),
            timestamp=datetime.now(timezone.utc),
            supplier_id=random.choice([None, "SUP-001", "SUP-002"]),
        )
        payload = event.to_kafka_payload()
        self._publisher.publish(payload, partition_key=product_id)
        logger.info("Generated warehouse event event_id=%s product_id=%s", event.event_id, product_id)
