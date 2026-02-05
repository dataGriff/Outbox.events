"""
Data models for the application
"""
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class EventType(str, Enum):
    """Business event types"""
    ORDER_CREATED = "order.created"
    ORDER_UPDATED = "order.updated"
    ORDER_CANCELLED = "order.cancelled"
    PAYMENT_PROCESSED = "payment.processed"
    PAYMENT_FAILED = "payment.failed"
    SHIPMENT_CREATED = "shipment.created"


class EventStatus(str, Enum):
    """Outbox event status"""
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"


class Order(BaseModel):
    """Order domain model"""
    order_id: str = Field(..., description="Unique order identifier")
    customer_id: str = Field(..., description="Customer identifier")
    items: list[Dict[str, Any]] = Field(..., description="Order items")
    total_amount: float = Field(..., description="Total order amount")
    status: str = Field(default="pending", description="Order status")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class OrderCreateRequest(BaseModel):
    """Request model for creating an order"""
    customer_id: str = Field(..., description="Customer identifier")
    items: list[Dict[str, Any]] = Field(..., description="Order items")
    total_amount: float = Field(..., description="Total order amount", gt=0)


class OrderResponse(BaseModel):
    """Response model for order operations"""
    order_id: str
    customer_id: str
    items: list[Dict[str, Any]]
    total_amount: float
    status: str
    created_at: datetime
    updated_at: datetime


class OutboxEvent(BaseModel):
    """Outbox event model for the transactional outbox pattern"""
    event_id: str = Field(..., description="Unique event identifier")
    event_type: EventType = Field(..., description="Type of business event")
    aggregate_id: str = Field(..., description="ID of the aggregate (e.g., order_id)")
    payload: Dict[str, Any] = Field(..., description="Event payload")
    status: EventStatus = Field(default=EventStatus.PENDING)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    published_at: Optional[datetime] = None
    retry_count: int = Field(default=0)


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    mongodb: str
    kafka: str
    timestamp: datetime
