import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from app.services.order_service import OrderService, OptimisticLockError, OrderNotFoundError
from app.models.schemas import CreateOrderRequest, LineItemSchema, ShippingAddressSchema, UpdateOrderRequest
from app.models.orm import Order, OutboxEvent, OrderStatus, OutboxEventType

@pytest.mark.asyncio
async def test_create_order_adds_order_and_outbox_event():
    # Arrange
    mock_session = AsyncMock()
    service = OrderService(mock_session)
    
    customer_id = uuid.uuid4()
    product_id = uuid.uuid4()
    
    req = CreateOrderRequest(
        customer_id=customer_id,
        line_items=[
            LineItemSchema(product_id=product_id, quantity=2, unit_price_cents=1000)
        ],
        currency="USD",
        shipping_address=ShippingAddressSchema(
            street="123 Test St",
            city="SF",
            state="CA",
            zip_code="94102",
            country="US"
        )
    )
    
    # Act
    order = await service.create_order(req)
    
    # Assert
    assert order.customer_id == customer_id
    assert order.total_amount_cents == 2000
    assert order.version == 1
    
    # Verify both Order and OutboxEvent were added to session
    added_objects = [call.args[0] for call in mock_session.add.call_args_list]
    assert len(added_objects) == 2
    
    # Check that we added an Order and an OutboxEvent
    order_obj = next(obj for obj in added_objects if isinstance(obj, Order))
    outbox_obj = next(obj for obj in added_objects if isinstance(obj, OutboxEvent))
    
    assert order_obj.customer_id == customer_id
    assert outbox_obj.aggregate_type == "orders"
    assert outbox_obj.aggregate_id == order.id
    assert outbox_obj.event_type == OutboxEventType.ORDER_CREATED
    assert outbox_obj.payload["order_id"] == str(order.id)
    
    # Verify flush was called twice
    assert mock_session.flush.call_count == 2


@pytest.mark.asyncio
async def test_update_order_version_mismatch_raises_optimistic_lock_error():
    # Arrange
    mock_session = AsyncMock()
    service = OrderService(mock_session)
    
    order_id = uuid.uuid4()
    existing_order = Order(
        id=order_id,
        customer_id=uuid.uuid4(),
        status=OrderStatus.PENDING,
        line_items=[],
        total_amount_cents=0,
        version=2,  # DB has version 2
    )
    
    # Mock database select result
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = existing_order
    mock_session.execute.return_value = mock_result
    
    # Client requests update with outdated version 1
    req = UpdateOrderRequest(
        status="CONFIRMED",
        version=1
    )
    
    # Act & Assert
    with pytest.raises(OptimisticLockError):
        await service.update_order(order_id, req)
    
    assert mock_session.flush.call_count == 0
