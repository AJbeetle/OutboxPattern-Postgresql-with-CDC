from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging
from app.db.session import engine
from app.routers import health, orders

setup_logging()
logger = get_logger(__name__)
settings = get_settings()


# ---------------------------------------------------------------------------
# Lifespan — startup/shutdown hooks
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "api_starting",
        app=settings.app_name,
        version=settings.app_version,
        environment=settings.environment,
    )
    yield
    # Dispose connection pool on shutdown
    await engine.dispose()
    logger.info("api_shutdown")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Order Service — Outbox + CDC Demo",
    description="""
## Production-grade Outbox Pattern with CDC

This API demonstrates:
- **Transactional Outbox Pattern** — order writes and outbox events are atomic
- **Optimistic Locking** — version field prevents lost updates
- **Soft Deletes** — orders are never hard-deleted for audit compliance
- **Distributed Tracing** — pass X-Trace-Id/X-Span-Id headers for end-to-end tracing

### CDC Flow
1. API writes order + outbox_event in one transaction
2. Debezium reads outbox_events via PostgreSQL WAL
3. Outbox Event Router SMT transforms CDC envelope → domain event
4. Domain event published to Kafka topic `outbox.event.orders`
5. ClickHouse sink consumes from Kafka → analytics DB
""",
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else [],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log every request with method, path, status, and duration."""
    import time
    start = time.perf_counter()

    # Bind request context to structlog for all logs within this request
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        method=request.method,
        path=request.url.path,
        trace_id=request.headers.get("X-Trace-Id"),
    )

    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000

    logger.info(
        "request_handled",
        status_code=response.status_code,
        duration_ms=round(duration_ms, 2),
    )
    return response


# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error("unhandled_exception", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(health.router)
app.include_router(orders.router, prefix="/api/v1")