from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .logging_config import setup_logging
from .metrics import prometheus_http_middleware
from .service import WarehouseConsumerService


setup_logging()
service = WarehouseConsumerService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(asyncio.to_thread(service.run_forever))
    try:
        yield
    finally:
        service.close()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Smart Warehouse Consumer Service", version="1.0.0", lifespan=lifespan)
app.middleware("http")(prometheus_http_middleware)


@app.get("/health")
def health(response: Response) -> dict[str, str]:
    if service.health():
        return {"status": "ok"}
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "unavailable"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
