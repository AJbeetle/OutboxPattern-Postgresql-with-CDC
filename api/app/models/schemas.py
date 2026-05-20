import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Line item schemas
# ---------------------------------------------------------------------------

class LineItemSchema(BaseModel):
    product_id: uuid.UUID
    quantity: int = Field(..., ge=1, le=10_000)
    unit_price_cents: int = Field(..., ge=0)

    @property
    def subtotal_cents(self) -> int:
        return self.quantity * self.unit_price_cents


class ShippingAddressSchema(BaseModel):
    street: str = Field(..., min_length=1, max_length=255)
    city: str = Field(..., min_length=1, max_length=100)
    state: str = Field(..., min_length=2, max_length=100)
    zip_code: str = Field(..., min_length=3, max_length=20)
    country: str = Field(..., min_length=2, max_length=3)  # ISO 3166-1 alpha-2 or alpha-3


# ---------------------------------------------------------------------------
# Order request schemas
# ---------------------------------------------------------------------------

class CreateOrderRequest(BaseModel):
    customer_id: uuid.UUID
    line_items: list[LineItemSchema] = Field(..., min_length=1)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    shipping_address: ShippingAddressSchema | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency")
    @classmethod
    def currency_uppercase(cls, v: str) -> str:
        return v.upper()

    @model_validator(mode="after")
    def validate_total(self) -> "CreateOrderRequest":
        total = sum(item.subtotal_cents for item in self.line_items)
        if total > 100_000_000:  # $1M limit
            raise ValueError("Order total exceeds maximum allowed amount")
        return self


class UpdateOrderRequest(BaseModel):
    status: str | None = None
    line_items: list[LineItemSchema] | None = None
    shipping_address: ShippingAddressSchema | None = None
    metadata: dict[str, Any] | None = None
    # Client must pass current version for optimistic locking
    version: int = Field(..., ge=1, description="Current order version for optimistic lock")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str | None) -> str | None:
        if v is None:
            return v
        valid = {"PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}
        if v.upper() not in valid:
            raise ValueError(f"Invalid status. Must be one of: {valid}")
        return v.upper()


# ---------------------------------------------------------------------------
# Order response schemas
# ---------------------------------------------------------------------------

class LineItemResponse(BaseModel):
    product_id: uuid.UUID
    quantity: int
    unit_price_cents: int


class OrderResponse(BaseModel):
    id: uuid.UUID
    customer_id: uuid.UUID
    status: str
    line_items: list[dict]
    total_amount_cents: int
    currency: str
    shipping_address: dict | None
    metadata: dict
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class OrderListResponse(BaseModel):
    items: list[OrderResponse]
    total: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Outbox event response (for monitoring endpoints)
# ---------------------------------------------------------------------------

class OutboxEventResponse(BaseModel):
    id: uuid.UUID
    aggregate_type: str
    aggregate_id: uuid.UUID
    event_type: str
    schema_version: int
    status: str
    retry_count: int
    created_at: datetime
    processed_at: datetime | None

    model_config = {"from_attributes": True}