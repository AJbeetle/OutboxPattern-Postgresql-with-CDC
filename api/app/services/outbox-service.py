"""
OutboxService — the heart of the outbox pattern.

Responsibility: given a business entity change, construct a well-formed
OutboxEvent and INSERT it in the same SQLAlchemy session (same transaction)
as the business entity write.

CRITICAL INVARIANT:
  session.add(order)          ← business write
  session.add(outbox_event)   ← outbox write
  await session.flush()       ← both go to DB in one atomic operation

If the transaction rolls back, BOTH writes are reverted. This is the guarantee
that makes the outbox pattern work: there is never a state where the business
entity is written but the event is not (or vice versa).
"""

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models.orm import OutboxEvent, OutboxEventType, Order

logger = get_logger(__name__)

AGGREGATE_TYPE_ORDERS = "orders"
SCHEMA_VERSION = 1


class OutboxService:
    def __init__(self, session: AsyncSession):
        self._session = session

    # -------------------------------------------------------------------------
    # Public interface — one method per domain event type
    # -------------------------------------------------------------------------

    async def record_order_created(
        self,
        order: Order,
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        payload = self._build_order_payload(order, deleted=False)
        return await self._insert_event(
            aggregate_id=order.id,
            event_type=OutboxEventType.ORDER_CREATED,
            payload=payload,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
        )

    async def record_order_updated(
        self,
        order: Order,
        changed_fields: list[str],
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
        causation_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        payload = self._build_order_payload(order, deleted=False)
        # Include which fields changed so consumers can filter irrelevant updates
        payload["changed_fields"] = changed_fields
        return await self._insert_event(
            aggregate_id=order.id,
            event_type=OutboxEventType.ORDER_UPDATED,
            payload=payload,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_order_deleted(
        self,
        order: Order,
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        # For deletes: include enough data for downstream consumers to clean up
        payload = self._build_order_payload(order, deleted=True)
        return await self._insert_event(
            aggregate_id=order.id,
            event_type=OutboxEventType.ORDER_DELETED,
            payload=payload,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
        )

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _build_order_payload(self, order: Order, deleted: bool) -> dict[str, Any]:
        """
        Build the event payload. This is what downstream consumers receive.

        SCHEMA VERSION 1 — bump schema_version in _insert_event when this changes.

        Rule: include ALL fields needed for consumers to act without calling back
        to the orders service. This avoids the "event-carried state transfer"
        anti-pattern where consumers do a follow-up GET request.
        """
        return {
            # Aggregate identity
            "order_id": str(order.id),
            "customer_id": str(order.customer_id),

            # Current state
            "status": order.status.value if hasattr(order.status, "value") else order.status,
            "line_items": order.line_items,
            "total_amount_cents": order.total_amount_cents,
            "currency": order.currency,
            "shipping_address": order.shipping_address,
            "metadata": order.metadata_,

            # Versioning
            "version": order.version,
            "is_deleted": deleted,

            # Timestamps (ISO 8601 strings for portable serialization)
            "order_created_at": order.created_at.isoformat() if order.created_at else None,
            "order_updated_at": order.updated_at.isoformat() if order.updated_at else None,
        }

    async def _insert_event(
        self,
        aggregate_id: uuid.UUID,
        event_type: OutboxEventType,
        payload: dict[str, Any],
        trace_id: str | None = None,
        span_id: str | None = None,
        correlation_id: uuid.UUID | None = None,
        causation_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        event = OutboxEvent(
            id=uuid.uuid4(),
            aggregate_type=AGGREGATE_TYPE_ORDERS,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            schema_version=SCHEMA_VERSION,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

        # Add to session — will be committed together with the business entity
        # in the calling service's transaction. DO NOT commit here.
        self._session.add(event)

        logger.debug(
            "outbox_event_queued",
            event_id=str(event.id),
            event_type=event_type.value,
            aggregate_id=str(aggregate_id),
            trace_id=trace_id,
        )

        return event