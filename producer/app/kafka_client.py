from __future__ import annotations

import json
import logging
import time
from typing import Any

from confluent_kafka import Producer

from .config import settings
from .schema import validate_against_schema


logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": settings.producer_acks,
                "enable.idempotence": True,
                "retries": settings.producer_max_retries,
                "retry.backoff.ms": int(settings.producer_backoff_seconds * 1000),
                "compression.type": "lz4",
            }
        )

    def publish(self, payload: dict[str, Any], partition_key: str) -> None:
        validate_against_schema(payload)
        value = json.dumps(payload).encode("utf-8")
        schema_version = str(payload.get("schema_version", 2)).encode("utf-8")
        headers = [("schema-subject", b"warehouse-events-value"), ("schema-version", schema_version)]

        delivery_error: Exception | None = None
        for attempt in range(1, settings.producer_max_retries + 1):
            try:
                self._producer.produce(
                    settings.kafka_topic,
                    key=partition_key.encode("utf-8"),
                    value=value,
                    headers=headers,
                    on_delivery=self._delivery_callback,
                )
                self._producer.flush(timeout=10)
                logger.info(
                    "Published event event_id=%s event_type=%s timestamp=%s",
                    payload["event_id"],
                    payload["event_type"],
                    payload["timestamp"],
                )
                return
            except BufferError as exc:
                delivery_error = exc
            except Exception as exc:
                delivery_error = exc

            sleep_seconds = settings.producer_backoff_seconds * (2 ** (attempt - 1))
            logger.warning("Kafka publish failed on attempt=%s error=%s", attempt, delivery_error)
            time.sleep(sleep_seconds)

        raise RuntimeError(f"Kafka publish failed after retries: {delivery_error}")

    @staticmethod
    def _delivery_callback(err: Exception | None, msg: Any) -> None:
        if err is not None:
            raise RuntimeError(f"Delivery failed: {err}")
