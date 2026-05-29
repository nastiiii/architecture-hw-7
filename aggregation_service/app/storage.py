from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, dict_factory

from .config import settings


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def consistency_level(name: str) -> int:
    return getattr(ConsistencyLevel, name.upper())


@dataclass
class InventoryState:
    available: int = 0
    reserved: int = 0
    last_event_ts: datetime | None = None


class CassandraStore:
    def __init__(self) -> None:
        contact_points = [point.strip() for point in settings.cassandra_contact_points.split(",") if point.strip()]
        self.cluster = Cluster(contact_points=contact_points)
        self.session: Session = self.cluster.connect(settings.cassandra_keyspace)
        self.session.row_factory = dict_factory
        self.read_cl = consistency_level(settings.cassandra_read_consistency)
        self.write_cl = consistency_level(settings.cassandra_write_consistency)
        self._prepare()

    def _prepare(self) -> None:
        self.select_processed = self.session.prepare("SELECT event_id FROM processed_events WHERE event_id = ?")
        self.select_processed.consistency_level = self.read_cl

        self.select_inventory = self.session.prepare(
            """
            SELECT available_quantity, reserved_quantity, last_event_ts
            FROM inventory_by_product_zone
            WHERE product_id = ? AND zone_id = ?
            """
        )
        self.select_inventory.consistency_level = self.read_cl
        self.select_product_zones = self.session.prepare(
            """
            SELECT zone_id, available_quantity, reserved_quantity, last_event_ts
            FROM inventory_by_product_zone
            WHERE product_id = ?
            """
        )
        self.select_product_zones.consistency_level = self.read_cl

        self.select_order = self.session.prepare("SELECT status, items FROM orders_by_id WHERE order_id = ?")
        self.select_order.consistency_level = self.read_cl

        self.upsert_product_zone = self.session.prepare(
            """
            INSERT INTO inventory_by_product_zone
            (product_id, zone_id, available_quantity, reserved_quantity, last_event_ts, last_event_id, supplier_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.upsert_by_product = self.session.prepare(
            """
            INSERT INTO inventory_by_product
            (product_id, total_available, total_reserved, last_event_ts, last_event_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self.upsert_by_zone = self.session.prepare(
            """
            INSERT INTO inventory_by_zone
            (zone_id, product_id, available_quantity, reserved_quantity, last_event_ts, last_event_id, supplier_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_processed = self.session.prepare(
            """
            INSERT INTO processed_events
            (event_id, event_type, product_id, processed_at, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_history = self.session.prepare(
            """
            INSERT INTO event_history_by_product
            (product_id, event_ts, event_id, event_type, zone_id, quantity, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.upsert_order = self.session.prepare(
            """
            INSERT INTO orders_by_id
            (order_id, status, items, created_at, updated_at, last_event_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        for statement in [
            self.upsert_product_zone,
            self.upsert_by_product,
            self.upsert_by_zone,
            self.insert_processed,
            self.insert_history,
            self.upsert_order,
        ]:
            statement.consistency_level = self.write_cl

    def close(self) -> None:
        self.cluster.shutdown()

    def ping(self) -> bool:
        try:
            self.session.execute("SELECT now() FROM system.local", timeout=3)
            return True
        except Exception:
            return False

    def is_processed(self, event_id: UUID) -> bool:
        return self.session.execute(self.select_processed, (event_id,)).one() is not None

    def read_inventory(self, product_id: str, zone_id: str) -> InventoryState:
        row = self.session.execute(self.select_inventory, (product_id, zone_id)).one()
        if row is None:
            return InventoryState()
        return InventoryState(
            available=int(row["available_quantity"] or 0),
            reserved=int(row["reserved_quantity"] or 0),
            last_event_ts=row["last_event_ts"],
        )

    def read_product_zones(self, product_id: str) -> dict[tuple[str, str], InventoryState]:
        rows = self.session.execute(self.select_product_zones, (product_id,))
        return {
            (product_id, row["zone_id"]): InventoryState(
                available=int(row["available_quantity"] or 0),
                reserved=int(row["reserved_quantity"] or 0),
                last_event_ts=row["last_event_ts"],
            )
            for row in rows
        }

    def read_order_items(self, order_id: str) -> list[dict[str, Any]]:
        row = self.session.execute(self.select_order, (order_id,)).one()
        if row is None:
            raise ValueError(f"Order {order_id} does not exist")
        return json.loads(row["items"])

    def apply_event_batch(
        self,
        *,
        event: dict[str, Any],
        changed_rows: dict[tuple[str, str], InventoryState],
        product_totals: dict[str, InventoryState],
        order_status: str | None,
        order_items: list[dict[str, Any]] | None,
        kafka_partition: int,
        kafka_offset: int,
    ) -> None:
        event_id = UUID(event["event_id"])
        event_ts = event["timestamp_dt"]
        updated_at = now_utc()
        batch = BatchStatement(consistency_level=self.write_cl)

        for (product_id, zone_id), state in changed_rows.items():
            supplier_id = event.get("supplier_id")
            batch.add(
                self.upsert_product_zone,
                (product_id, zone_id, state.available, state.reserved, event_ts, event_id, supplier_id, updated_at),
            )
            batch.add(
                self.upsert_by_zone,
                (zone_id, product_id, state.available, state.reserved, event_ts, event_id, supplier_id, updated_at),
            )

        for product_id, total in product_totals.items():
            batch.add(self.upsert_by_product, (product_id, total.available, total.reserved, event_ts, event_id, updated_at))
            batch.add(
                self.insert_history,
                (
                    product_id,
                    event_ts,
                    event_id,
                    event["event_type"],
                    event.get("zone_id") or event.get("from_zone_id") or event.get("to_zone_id"),
                    event.get("quantity") or event.get("counted_quantity") or 0,
                    json.dumps(event, default=str),
                ),
            )

        if order_status is not None and event.get("order_id"):
            batch.add(
                self.upsert_order,
                (
                    event["order_id"],
                    order_status,
                    json.dumps(order_items or event.get("items") or []),
                    event_ts,
                    updated_at,
                    event_id,
                ),
            )

        batch.add(
            self.insert_processed,
            (event_id, event["event_type"], event.get("product_id") or "", updated_at, kafka_partition, kafka_offset),
        )
        self.session.execute(batch)
