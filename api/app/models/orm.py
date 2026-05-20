import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Enum mirrors — keep in sync with SQL ENUMs in 001_init_schema.sql
# ---------------------------------------------------------------------------

class OrderStatus(str, enum.Enum):
    PENDING     = "PENDING"
    CONFIRMED   = "CONFIRMED"
    PROCESSING  = "PROCESSING"
    SHIPPED     = "SHIPPED"
    DELIVERED   = "DELIVERED"
    CANCELLED   = "CANCELLED"
    REFUNDED    = "REFUNDED"


class OutboxEventType(str, enum.Enum):
    ORDER_CREATED   = "ORDER_CREATED"
    ORDER_UPDATED   = "ORDER_UPDATED"
    ORDER_DELETED   = "ORDER_DELETED"


class OutboxStatus(str, enum.Enum):
    PENDING     = "PENDING"
    PROCESSED   = "PROCESSED"
    FAILED      = "FAILED"


# ---------------------------------------------------------------------------
# Order model
# ---------------------------------------------------------------------------

class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, name="order_status", create_type=False),
        nullable=False,
        default=OrderStatus.PENDING,
    )
    line_items: Mapped[list[dict]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    total_amount_cents: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    currency: Mapped[str] = mapped_column(
        String(3), nullable=False, default="USD"
    )
    shipping_address: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("total_amount_cents >= 0", name="ck_orders_positive_amount"),
        Index("idx_orders_status_active", "status", postgresql_where="deleted_at IS NULL"),
    )

    def __repr__(self) -> str:
        return f"<Order id={self.id} status={self.status} customer={self.customer_id}>"


# ---------------------------------------------------------------------------
# OutboxEvent model
# ---------------------------------------------------------------------------

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[OutboxEventType] = mapped_column(
        Enum(OutboxEventType, name="outbox_event_type", create_type=False),
        nullable=False,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Distributed tracing
    trace_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    span_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Correlation
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    causation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    # CDC status
    status: Mapped[OutboxStatus] = mapped_column(
        Enum(OutboxStatus, name="outbox_status", create_type=False),
        nullable=False,
        default=OutboxStatus.PENDING,
    )
    error_details: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("idx_outbox_pending", "status", postgresql_where="status = 'PENDING'"),
        Index("idx_outbox_aggregate_lookup", "aggregate_type", "aggregate_id"),
    )

    def __repr__(self) -> str:
        return f"<OutboxEvent id={self.id} type={self.event_type} agg={self.aggregate_id}>"