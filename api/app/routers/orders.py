import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.session import get_db
from app.models.schemas import (
    CreateOrderRequest,
    OrderListResponse,
    OrderResponse,
    UpdateOrderRequest,
)
from app.services.order_service import (
    OptimisticLockError,
    OrderAlreadyDeletedError,
    OrderNotFoundError,
    OrderService,
)

router = APIRouter(prefix="/orders", tags=["orders"])
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Dependency — extract trace context from headers (OpenTelemetry W3C Trace Context)
# ---------------------------------------------------------------------------

def get_trace_context(
    request: Request,
    x_trace_id: Annotated[str | None, Header(alias="X-Trace-Id")] = None,
    x_span_id: Annotated[str | None, Header(alias="X-Span-Id")] = None,
    x_correlation_id: Annotated[str | None, Header(alias="X-Correlation-Id")] = None,
) -> dict:
    return {
        "trace_id": x_trace_id,
        "span_id": x_span_id,
        "correlation_id": uuid.UUID(x_correlation_id) if x_correlation_id else None,
    }


TraceContext = Annotated[dict, Depends(get_trace_context)]
DB = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# POST /orders — create a new order
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new order",
    description="""
    Creates an order and atomically writes an ORDER_CREATED event to the outbox.
    The outbox event will be picked up by Debezium CDC and published to Kafka.
    """,
)
async def create_order(
    body: CreateOrderRequest,
    db: DB,
    trace: TraceContext,
):
    svc = OrderService(db)
    try:
        order = await svc.create_order(
            req=body,
            trace_id=trace["trace_id"],
            span_id=trace["span_id"],
            correlation_id=trace["correlation_id"],
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

    return _to_response(order)


# ---------------------------------------------------------------------------
# GET /orders — list orders with filtering and pagination
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=OrderListResponse,
    summary="List orders",
)
async def list_orders(
    db: DB,
    customer_id: uuid.UUID | None = Query(default=None),
    order_status: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    svc = OrderService(db)
    try:
        orders, total = await svc.list_orders(
            customer_id=customer_id,
            status=order_status,
            page=page,
            page_size=page_size,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

    return OrderListResponse(
        items=[_to_response(o) for o in orders],
        total=total,
        page=page,
        page_size=page_size,
    )


# ---------------------------------------------------------------------------
# GET /orders/{order_id} — get single order
# ---------------------------------------------------------------------------

@router.get(
    "/{order_id}",
    response_model=OrderResponse,
    summary="Get order by ID",
)
async def get_order(order_id: uuid.UUID, db: DB):
    svc = OrderService(db)
    try:
        order = await svc.get_order(order_id)
    except OrderNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order {order_id} not found")
    except OrderAlreadyDeletedError:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail=f"Order {order_id} has been deleted")

    return _to_response(order)


# ---------------------------------------------------------------------------
# PUT /orders/{order_id} — update order (partial, with optimistic locking)
# ---------------------------------------------------------------------------

@router.put(
    "/{order_id}",
    response_model=OrderResponse,
    summary="Update an order",
    description="""
    Updates order fields. Requires `version` in the request body for optimistic
    locking — pass the current version from your last GET response.
    Returns 409 if another process updated the order concurrently.
    """,
)
async def update_order(
    order_id: uuid.UUID,
    body: UpdateOrderRequest,
    db: DB,
    trace: TraceContext,
):
    svc = OrderService(db)
    try:
        order = await svc.update_order(
            order_id=order_id,
            req=body,
            trace_id=trace["trace_id"],
            span_id=trace["span_id"],
            correlation_id=trace["correlation_id"],
        )
    except OrderNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order {order_id} not found")
    except OrderAlreadyDeletedError:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail=f"Order {order_id} has been deleted")
    except OptimisticLockError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

    return _to_response(order)


# ---------------------------------------------------------------------------
# DELETE /orders/{order_id} — soft delete
# ---------------------------------------------------------------------------

@router.delete(
    "/{order_id}",
    response_model=OrderResponse,
    summary="Delete (soft) an order",
    description="""
    Soft-deletes an order by setting deleted_at and status=CANCELLED.
    Atomically writes ORDER_DELETED event to the outbox.
    Returns 410 if already deleted.
    """,
)
async def delete_order(
    order_id: uuid.UUID,
    db: DB,
    trace: TraceContext,
):
    svc = OrderService(db)
    try:
        order = await svc.delete_order(
            order_id=order_id,
            trace_id=trace["trace_id"],
            span_id=trace["span_id"],
            correlation_id=trace["correlation_id"],
        )
    except OrderNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order {order_id} not found")
    except OrderAlreadyDeletedError:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail=f"Order {order_id} has been deleted")

    return _to_response(order)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _to_response(order) -> OrderResponse:
    return OrderResponse(
        id=order.id,
        customer_id=order.customer_id,
        status=order.status.value if hasattr(order.status, "value") else order.status,
        line_items=order.line_items,
        total_amount_cents=order.total_amount_cents,
        currency=order.currency,
        shipping_address=order.shipping_address,
        metadata=order.metadata_,
        version=order.version,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )