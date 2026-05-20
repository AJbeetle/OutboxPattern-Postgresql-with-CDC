"""
OrderService — orchestrates order CRUD with atomic outbox event recording.

Pattern:
  1. Validate business rules
  2. Write to orders table
  3. Write to outbox_events table  ← same session, same transaction
  4. flush() — both writes go to DB atomically (no commit yet)
  5. FastAPI's get_db() dependency commits the session after the handler returns
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models.orm import Order, OrderStatus
from app.models.schemas import CreateOrderRequest, UpdateOrderRequest
from app.services.outbox_service import OutboxService

logger = get_logger(__name__)


class OptimisticLockError(Exception):
    """Raised when a concurrent update is detected via version mismatch."""
    pass


class OrderNotFoundError(Exception):
    pass


class OrderAlreadyDeletedError(Exception):
    pass


class OrderService:
    def __init__(self, session: AsyncSession):
        self._session = session
        self._outbox = OutboxService(session)

    # -------------------------------------------------------------------------
    # Create order
    # -------------------------------------------------------------------------

    async def create_order(
        self,
        req: CreateOrderRequest,
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> Order:
        # Compute total from line items
        total_cents = sum(
            item.quantity * item.unit_price_cents for item in req.line_items
        )

        order = Order(
            id=uuid.uuid4(),
            customer_id=req.customer_id,
            status=OrderStatus.PENDING,
            line_items=[item.model_dump(mode="json") for item in req.line_items],
            total_amount_cents=total_cents,
            currency=req.currency,
            shipping_address=req.shipping_address.model_dump(mode="json") if req.shipping_address else None,
            metadata_=req.metadata,
            version=1,
        )

        # ── ATOMIC WRITE ──────────────────────────────────────────────────────
        # Both add() calls enqueue writes into the same SQLAlchemy unit-of-work.
        # flush() sends both INSERTs to Postgres in one network round-trip,
        # within the same transaction. If either fails, the transaction rolls back.
        self._session.add(order)
        await self._session.flush()  # get server-generated created_at/updated_at

        await self._outbox.record_order_created(
            order=order,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
        )

        await self._session.flush()  # flush outbox event too
        # ─────────────────────────────────────────────────────────────────────

        logger.info(
            "order_created",
            order_id=str(order.id),
            customer_id=str(order.customer_id),
            total_cents=total_cents,
            trace_id=trace_id,
        )

        return order

    # -------------------------------------------------------------------------
    # Update order
    # -------------------------------------------------------------------------

    async def update_order(
        self,
        order_id: uuid.UUID,
        req: UpdateOrderRequest,
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> Order:
        order = await self._get_active_order(order_id)

        # Optimistic locking — reject if client's version doesn't match DB
        if order.version != req.version:
            raise OptimisticLockError(
                f"Order version conflict: expected {req.version}, found {order.version}. "
                "Fetch the latest order and retry."
            )

        changed_fields: list[str] = []

        if req.status is not None and req.status != order.status:
            order.status = OrderStatus(req.status)
            changed_fields.append("status")

        if req.line_items is not None:
            order.line_items = [item.model_dump(mode="json") for item in req.line_items]
            order.total_amount_cents = sum(
                item.quantity * item.unit_price_cents for item in req.line_items
            )
            changed_fields.extend(["line_items", "total_amount_cents"])

        if req.shipping_address is not None:
            order.shipping_address = req.shipping_address.model_dump(mode="json")
            changed_fields.append("shipping_address")

        if req.metadata is not None:
            # Merge metadata rather than replace (allows partial updates)
            order.metadata_ = {**order.metadata_, **req.metadata}
            changed_fields.append("metadata")

        if not changed_fields:
            return order  # no-op update, skip flush and outbox event

        # Increment version for next optimistic lock check
        order.version += 1

        # ── ATOMIC WRITE ──────────────────────────────────────────────────────
        await self._session.flush()

        await self._outbox.record_order_updated(
            order=order,
            changed_fields=changed_fields,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
        )

        await self._session.flush()
        # ─────────────────────────────────────────────────────────────────────

        logger.info(
            "order_updated",
            order_id=str(order_id),
            changed_fields=changed_fields,
            new_version=order.version,
            trace_id=trace_id,
        )

        return order

    # -------------------------------------------------------------------------
    # Delete order (soft delete)
    # -------------------------------------------------------------------------

    async def delete_order(
        self,
        order_id: uuid.UUID,
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> Order:
        order = await self._get_active_order(order_id)

        # Soft delete — set deleted_at and status
        order.deleted_at = datetime.now(timezone.utc)
        order.status = OrderStatus.CANCELLED
        order.version += 1

        # ── ATOMIC WRITE ──────────────────────────────────────────────────────
        await self._session.flush()

        await self._outbox.record_order_deleted(
            order=order,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
        )

        await self._session.flush()
        # ─────────────────────────────────────────────────────────────────────

        logger.info(
            "order_deleted",
            order_id=str(order_id),
            trace_id=trace_id,
        )

        return order

    # -------------------------------------------------------------------------
    # Read operations
    # -------------------------------------------------------------------------

    async def get_order(self, order_id: uuid.UUID) -> Order:
        return await self._get_active_order(order_id)

    async def list_orders(
        self,
        customer_id: uuid.UUID | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Order], int]:
        stmt = select(Order).where(Order.deleted_at.is_(None))

        if customer_id:
            stmt = stmt.where(Order.customer_id == customer_id)
        if status:
            stmt = stmt.where(Order.status == OrderStatus(status))

        # Count total (without pagination)
        from sqlalchemy import func, select as sa_select
        count_stmt = sa_select(func.count()).select_from(stmt.subquery())
        total = (await self._session.execute(count_stmt)).scalar_one()

        # Apply pagination
        stmt = (
            stmt.order_by(Order.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await self._session.execute(stmt)
        orders = result.scalars().all()

        return list(orders), total

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    async def _get_active_order(self, order_id: uuid.UUID) -> Order:
        result = await self._session.execute(
            select(Order).where(
                Order.id == order_id,
                Order.deleted_at.is_(None),
            )
        )
        order = result.scalar_one_or_none()

        if order is None:
            # Check if it exists but is deleted
            deleted_result = await self._session.execute(
                select(Order).where(Order.id == order_id, Order.deleted_at.isnot(None))
            )
            if deleted_result.scalar_one_or_none():
                raise OrderAlreadyDeletedError(f"Order {order_id} has been deleted")
            raise OrderNotFoundError(f"Order {order_id} not found")

        return order