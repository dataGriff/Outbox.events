"""
Domain models for the booking system
"""
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, validator
from enum import Enum
import uuid


class BookingStatus(str, Enum):
    """Status enumeration for bookings"""
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    PENDING = "pending"


class EventCategory(str, Enum):
    """Categories of domain events"""
    BOOKING_CONFIRMED = "booking.confirmed"
    BOOKING_CANCELLED = "booking.cancelled"
    BOOKING_COMPLETED = "booking.completed"
    PAYMENT_SUCCESS = "payment.success"
    PAYMENT_DECLINED = "payment.declined"


class OutboxState(str, Enum):
    """State of events in the outbox"""
    AWAITING = "awaiting"
    DISPATCHED = "dispatched"
    ERROR = "error"


class BookingItem(BaseModel):
    """Individual item in a booking"""
    item_identifier: str = Field(..., description="Item ID")
    item_quantity: int = Field(..., ge=1, description="Quantity")
    unit_cost: float = Field(..., gt=0, description="Cost per unit")
    
    @validator('unit_cost')
    def validate_cost(cls, value):
        return round(value, 2)


class BookingRequest(BaseModel):
    """Request payload for creating a booking"""
    client_identifier: str = Field(..., min_length=3, description="Client ID")
    booking_items: List[BookingItem] = Field(..., min_items=1, description="Items to book")
    notes: Optional[str] = Field(None, max_length=500, description="Additional notes")
    
    @property
    def calculated_total(self) -> float:
        """Calculate total amount"""
        return round(sum(item.item_quantity * item.unit_cost for item in self.booking_items), 2)


class BookingRecord(BaseModel):
    """Complete booking record"""
    booking_ref: str = Field(..., description="Booking reference")
    client_identifier: str
    booking_items: List[BookingItem]
    total_cost: float
    booking_status: BookingStatus
    notes: Optional[str] = None
    timestamp_created: datetime = Field(default_factory=datetime.utcnow)
    timestamp_modified: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventRecord(BaseModel):
    """Event stored in outbox table"""
    event_ref: str = Field(..., description="Event reference ID")
    category: EventCategory
    entity_id: str = Field(..., description="Related entity ID")
    event_data: Dict[str, Any]
    current_state: OutboxState = OutboxState.AWAITING
    timestamp_created: datetime = Field(default_factory=datetime.utcnow)
    timestamp_dispatched: Optional[datetime] = None
    attempt_count: int = 0
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SystemHealth(BaseModel):
    """Health status response"""
    overall: str
    database_status: str
    broker_status: str
    checked_at: datetime = Field(default_factory=datetime.utcnow)
