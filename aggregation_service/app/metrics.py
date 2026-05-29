from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from prometheus_client import Counter, Histogram


HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
HTTP_ERRORS = Counter(
    "http_request_errors_total",
    "Total HTTP request errors",
    ["method", "endpoint", "error_type"],
)
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)


async def prometheus_http_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    started = time.perf_counter()
    endpoint = request.url.path
    status = "500"
    try:
        response = await call_next(request)
        status = str(response.status_code)
        route = request.scope.get("route")
        endpoint = getattr(route, "path", endpoint)
        if response.status_code >= 500:
            HTTP_ERRORS.labels(request.method, endpoint, "server_error").inc()
        elif response.status_code >= 400:
            HTTP_ERRORS.labels(request.method, endpoint, "client_error").inc()
        return response
    except Exception:
        HTTP_ERRORS.labels(request.method, endpoint, "unhandled_exception").inc()
        raise
    finally:
        HTTP_REQUESTS.labels(request.method, endpoint, status).inc()
        HTTP_DURATION.labels(request.method, endpoint).observe(time.perf_counter() - started)
