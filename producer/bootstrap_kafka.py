from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, RESOURCE_TOPIC
from confluent_kafka.error import KafkaException


ROOT = Path(__file__).resolve().parent
SCHEMA_V1_PATH = ROOT / "app" / "schemas" / "warehouse_event_v1.avsc"
SCHEMA_V2_PATH = ROOT / "app" / "schemas" / "warehouse_event_v2.avsc"


def wait_for_topic_admin(admin: AdminClient, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Kafka admin API is not reachable")


def ensure_topic(admin: AdminClient, topic: str, partitions: int, replication_factor: int, min_isr: int) -> None:
    metadata = admin.list_topics(timeout=10)
    if topic not in metadata.topics:
        futures = admin.create_topics(
            [
                NewTopic(
                    topic=topic,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    config={"min.insync.replicas": str(min_isr)},
                )
            ]
        )
        try:
            futures[topic].result()
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise

    resources = [ConfigResource(RESOURCE_TOPIC, topic, set_config={"min.insync.replicas": str(min_isr)})]
    futures = admin.alter_configs(resources)
    for future in futures.values():
        future.result()


def wait_for_schema_registry(base_url: str, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(f"{base_url}/subjects", timeout=5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Schema Registry is not reachable")


def set_compatibility(base_url: str) -> None:
    response = requests.put(
        f"{base_url}/config/warehouse-events-value",
        json={"compatibility": "BACKWARD"},
        timeout=10,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    response.raise_for_status()


def register_schema(base_url: str, schema_path: Path) -> None:
    schema_payload = {"schema": json.dumps(json.loads(schema_path.read_text(encoding="utf-8")))}
    response = requests.post(
        f"{base_url}/subjects/warehouse-events-value/versions",
        json=schema_payload,
        timeout=10,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    if response.status_code == 409:
        raise RuntimeError(f"Schema compatibility check failed: {response.text}")
    if response.status_code == 422:
        raise RuntimeError(f"Schema registry rejected schema: {response.text}")
    response.raise_for_status()


def main() -> None:
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    schema_registry_url = os.environ["SCHEMA_REGISTRY_URL"]
    topic = os.environ.get("KAFKA_TOPIC", "warehouse-events")
    dlq_topic = os.environ.get("KAFKA_DLQ_TOPIC", "warehouse-events-dlq")
    partitions = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "3"))
    replication_factor = int(os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "2"))
    min_isr = int(os.environ.get("KAFKA_TOPIC_MIN_ISR", "1"))

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    wait_for_topic_admin(admin)
    wait_for_schema_registry(schema_registry_url)
    ensure_topic(admin, topic, partitions, replication_factor, min_isr)
    ensure_topic(admin, dlq_topic, partitions, replication_factor, min_isr)
    set_compatibility(schema_registry_url)
    register_schema(schema_registry_url, SCHEMA_V1_PATH)
    register_schema(schema_registry_url, SCHEMA_V2_PATH)
    print(f"Initialized topic={topic}, dlq={dlq_topic}, partitions={partitions}, rf={replication_factor}, min_isr={min_isr}")


if __name__ == "__main__":
    main()
