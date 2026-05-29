from __future__ import annotations

import json
import logging
import time
import traceback
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, Producer
from dateutil.parser import isoparse
from prometheus_client import Counter, Gauge, Histogram

from .config import settings
from .storage import CassandraStore, InventoryState, now_utc


logger = logging.getLogger(__name__)

EVENTS_PROCESSED = Counter("events_processed_total", "Processed warehouse events", ["event_type"])
PROCESSING_DURATION = Histogram("event_processing_duration_seconds", "Warehouse event processing duration")
CASSANDRA_WRITE_ERRORS = Counter("cassandra_write_errors_total", "Cassandra write errors")
CONSUMER_LAG = Gauge("consumer_lag", "Kafka consumer lag by partition", ["partition"])


class WarehouseConsumerService:
    def __init__(self) -> None:
        self.store = CassandraStore()
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        self.dlq = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
        self.running = False
        self.kafka_connected = False

    def close(self) -> None:
        self.running = False
        self.consumer.close()
        self.store.close()

    def health(self) -> bool:
        return self.kafka_connected and self.store.ping()

    def run_forever(self) -> None:
        self.running = True
        self.consumer.subscribe([settings.kafka_topic])
        logger.info("Consumer subscribed topic=%s group_id=%s", settings.kafka_topic, settings.kafka_group_id)
        while self.running:
            message = self.consumer.poll(1.0)
            if message is None:
                self.kafka_connected = True
                self._update_lag()
                continue
            if message.error():
                self.kafka_connected = False
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error=%s", message.error())
                continue

            self.kafka_connected = True
            try:
                self._handle_message(message)
                self.consumer.commit(message=message, asynchronous=False)
            except Exception as exc:
                logger.exception("Failed to process event offset=%s partition=%s", message.offset(), message.partition())
                self._publish_dlq(message, exc)
                self.consumer.commit(message=message, asynchronous=False)
            finally:
                self._update_lag()

    def _handle_message(self, message: Any) -> None:
        started = time.perf_counter()
        event = json.loads(message.value().decode("utf-8"))
        event["timestamp_dt"] = isoparse(event["timestamp"]).astimezone(timezone.utc)
        event_id = UUID(event["event_id"])
        event_type = event["event_type"]

        if self.store.is_processed(event_id):
            logger.info(
                "Duplicate event skipped event_id=%s event_type=%s partition=%s offset=%s",
                event_id,
                event_type,
                message.partition(),
                message.offset(),
            )
            return

        self._validate(event)
        changed_rows, order_status, order_items = self._build_state_changes(event)
        if not changed_rows and order_status is None:
            self.store.apply_event_batch(
                event=event,
                changed_rows={},
                product_totals={},
                order_status=None,
                order_items=None,
                kafka_partition=message.partition(),
                kafka_offset=message.offset(),
            )
            logger.info("Old event ignored event_id=%s event_type=%s", event_id, event_type)
        else:
            product_totals = self._build_product_totals(changed_rows)
            try:
                self.store.apply_event_batch(
                    event=event,
                    changed_rows=changed_rows,
                    product_totals=product_totals,
                    order_status=order_status,
                    order_items=order_items,
                    kafka_partition=message.partition(),
                    kafka_offset=message.offset(),
                )
            except Exception:
                CASSANDRA_WRITE_ERRORS.inc()
                raise

        EVENTS_PROCESSED.labels(event_type=event_type).inc()
        PROCESSING_DURATION.observe(time.perf_counter() - started)
        logger.info(
            "Processed event event_id=%s event_type=%s partition=%s offset=%s",
            event_id,
            event_type,
            message.partition(),
            message.offset(),
        )

    def _validate(self, event: dict[str, Any]) -> None:
        event_type = event["event_type"]
        quantity = event.get("quantity")
        if event_type != "INVENTORY_COUNTED" and event_type not in {"ORDER_CREATED", "ORDER_COMPLETED"}:
            if quantity is None or quantity <= 0:
                raise ValueError(f"Invalid quantity: {quantity} (must be positive)")
        if event_type == "INVENTORY_COUNTED" and (event.get("counted_quantity") is None or event["counted_quantity"] < 0):
            raise ValueError("Invalid counted_quantity (must be non-negative)")

    def _build_state_changes(self, event: dict[str, Any]) -> tuple[dict[tuple[str, str], InventoryState], str | None, list[dict[str, Any]] | None]:
        event_type = event["event_type"]
        if event_type == "ORDER_CREATED":
            changes: dict[tuple[str, str], InventoryState] = {}
            for item in event["items"]:
                changes.update(self._reserve(item["product_id"], item["zone_id"], item["quantity"], event["timestamp_dt"]))
            return changes, "CREATED", event["items"]
        if event_type == "ORDER_COMPLETED":
            items = self.store.read_order_items(event["order_id"])
            changes = {}
            for item in items:
                changes.update(self._complete_reserved(item["product_id"], item["zone_id"], item["quantity"], event["timestamp_dt"]))
            return changes, "COMPLETED", items

        product_id = event["product_id"]
        if event_type == "PRODUCT_RECEIVED":
            return self._change_available(product_id, event["zone_id"], event["quantity"], event["timestamp_dt"]), None, None
        if event_type == "PRODUCT_SHIPPED":
            return self._change_available(product_id, event["zone_id"], -event["quantity"], event["timestamp_dt"]), None, None
        if event_type == "PRODUCT_RESERVED":
            return self._reserve(product_id, event["zone_id"], event["quantity"], event["timestamp_dt"]), None, None
        if event_type == "PRODUCT_RELEASED":
            return self._release(product_id, event["zone_id"], event["quantity"], event["timestamp_dt"]), None, None
        if event_type == "INVENTORY_COUNTED":
            return self._count(product_id, event["zone_id"], event["counted_quantity"], event["timestamp_dt"]), None, None
        if event_type == "PRODUCT_MOVED":
            changes = self._change_available(product_id, event["from_zone_id"], -event["quantity"], event["timestamp_dt"])
            changes.update(self._change_available(product_id, event["to_zone_id"], event["quantity"], event["timestamp_dt"]))
            return changes, None, None
        raise ValueError(f"Unsupported event_type: {event_type}")

    def _should_ignore_old(self, state: InventoryState, event_ts: datetime) -> bool:
        return state.last_event_ts is not None and event_ts <= state.last_event_ts.replace(tzinfo=timezone.utc)

    def _change_available(self, product_id: str, zone_id: str, delta: int, event_ts: datetime) -> dict[tuple[str, str], InventoryState]:
        state = self.store.read_inventory(product_id, zone_id)
        if self._should_ignore_old(state, event_ts):
            return {}
        next_available = state.available + delta
        if next_available < 0:
            raise ValueError(f"Insufficient available stock product_id={product_id} zone_id={zone_id}")
        return {(product_id, zone_id): InventoryState(next_available, state.reserved, event_ts)}

    def _reserve(self, product_id: str, zone_id: str, quantity: int, event_ts: datetime) -> dict[tuple[str, str], InventoryState]:
        state = self.store.read_inventory(product_id, zone_id)
        if self._should_ignore_old(state, event_ts):
            return {}
        if state.available < quantity:
            raise ValueError(f"Insufficient available stock to reserve product_id={product_id} zone_id={zone_id}")
        return {(product_id, zone_id): InventoryState(state.available - quantity, state.reserved + quantity, event_ts)}

    def _release(self, product_id: str, zone_id: str, quantity: int, event_ts: datetime) -> dict[tuple[str, str], InventoryState]:
        state = self.store.read_inventory(product_id, zone_id)
        if self._should_ignore_old(state, event_ts):
            return {}
        if state.reserved < quantity:
            raise ValueError(f"Insufficient reserved stock to release product_id={product_id} zone_id={zone_id}")
        return {(product_id, zone_id): InventoryState(state.available + quantity, state.reserved - quantity, event_ts)}

    def _complete_reserved(self, product_id: str, zone_id: str, quantity: int, event_ts: datetime) -> dict[tuple[str, str], InventoryState]:
        state = self.store.read_inventory(product_id, zone_id)
        if self._should_ignore_old(state, event_ts):
            return {}
        if state.reserved < quantity:
            raise ValueError(f"Insufficient reserved stock to ship product_id={product_id} zone_id={zone_id}")
        return {(product_id, zone_id): InventoryState(state.available, state.reserved - quantity, event_ts)}

    def _count(self, product_id: str, zone_id: str, counted_quantity: int, event_ts: datetime) -> dict[tuple[str, str], InventoryState]:
        state = self.store.read_inventory(product_id, zone_id)
        if self._should_ignore_old(state, event_ts):
            return {}
        return {(product_id, zone_id): InventoryState(counted_quantity, state.reserved, event_ts)}

    def _build_product_totals(self, changed_rows: dict[tuple[str, str], InventoryState]) -> dict[str, InventoryState]:
        totals: dict[str, InventoryState] = {}
        for product_id, _ in changed_rows:
            zones = self.store.read_product_zones(product_id)
            for key, state in changed_rows.items():
                if key[0] == product_id:
                    zones[key] = state
            total = InventoryState()
            for state in zones.values():
                total.available += state.available
                total.reserved += state.reserved
            totals[product_id] = total
        return totals

    def _publish_dlq(self, message: Any, exc: Exception) -> None:
        try:
            original = json.loads(message.value().decode("utf-8"))
        except Exception:
            original = {"raw_value": message.value().decode("utf-8", errors="replace")}

        dlq_payload = {
            "original_event": original,
            "error_reason": str(exc),
            "error_code": "VALIDATION_ERROR" if isinstance(exc, ValueError) else "PROCESSING_ERROR",
            "stacktrace": traceback.format_exc(),
            "failed_at": now_utc().isoformat(),
            "kafka_metadata": {"partition": message.partition(), "offset": message.offset()},
        }
        self.dlq.produce(settings.kafka_dlq_topic, value=json.dumps(dlq_payload, default=str).encode("utf-8"))
        self.dlq.flush(10)

    def _update_lag(self) -> None:
        assignment = self.consumer.assignment()
        positions = {position.partition: position.offset for position in self.consumer.position(assignment)}
        for partition in assignment:
            low, high = self.consumer.get_watermark_offsets(partition, timeout=2, cached=False)
            current_offset = positions.get(partition.partition, low)
            if current_offset < 0:
                current_offset = low
            lag = max(0, high - current_offset)
            CONSUMER_LAG.labels(partition=str(partition.partition)).set(lag)
