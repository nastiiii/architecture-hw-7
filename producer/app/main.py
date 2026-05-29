from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import settings
from .generator import SyntheticEventGenerator
from .kafka_client import KafkaPublisher
from .logging_config import setup_logging
from .metrics import prometheus_http_middleware
from .models import WarehouseEventIn, WarehouseEventOut


setup_logging()
publisher = KafkaPublisher()
generator = SyntheticEventGenerator(publisher, settings.generator_interval_seconds)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = None
    if settings.generator_enabled:
        task = asyncio.create_task(generator.run())
    try:
        yield
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title="Smart Warehouse WMS Producer", version="1.0.0", lifespan=lifespan)
app.middleware("http")(prometheus_http_middleware)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/events")
def publish_event(event: WarehouseEventIn) -> dict[str, str]:
    try:
        payload = WarehouseEventOut(**event.model_dump()).to_kafka_payload()
        partition_key = event.product_id or event.order_id or event.items[0].product_id
        publisher.publish(payload, partition_key=partition_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"failed to publish event: {exc}") from exc
    return {"event_id": str(event.event_id)}


@app.post("/events/raw")
def publish_raw_event(event: dict[str, Any] = Body(...)) -> dict[str, str]:
    try:
        for optional_field in ["product_id", "zone_id", "from_zone_id", "to_zone_id", "quantity", "counted_quantity", "order_id", "supplier_id"]:
            event.setdefault(optional_field, None)
        event.setdefault("items", [])
        event.setdefault("schema_version", 2)
        partition_key = event.get("product_id") or event.get("order_id") or "raw"
        publisher.publish(event, partition_key=partition_key)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"failed to publish raw event: {exc}") from exc
    return {"event_id": str(event.get("event_id", ""))}


@app.post("/generator/enable")
def enable_generator() -> dict[str, bool]:
    generator.set_enabled(True)
    return {"enabled": True}


@app.post("/generator/disable")
def disable_generator() -> dict[str, bool]:
    generator.set_enabled(False)
    return {"enabled": False}
