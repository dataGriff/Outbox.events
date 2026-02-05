"""
Main FastAPI application implementing outbox pattern
"""
import logging
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from src.config.settings import get_configuration
from src.app.database import repository_instance
from src.app.kafka_service import broker_instance
from src.app.outbox_processor import worker_instance
from src.app.telemetry import configure_observability, apply_fastapi_instrumentation
from src.app.models import (
    BookingRequest, BookingRecord, BookingStatus,
    EventRecord, EventCategory, SystemHealth
)

# Logging setup
config = get_configuration()
logging.basicConfig(
    level=getattr(logging, config.logging_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Prometheus metrics
bookings_counter = Counter('booking_transactions_total', 'Total bookings created')
booking_errors_counter = Counter('booking_errors_total', 'Total booking failures')
events_counter = Counter('domain_events_total', 'Domain events dispatched', ['category'])
latency_histogram = Histogram('api_latency_seconds', 'API request latency', ['operation', 'path'])


@asynccontextmanager
async def application_lifecycle(app: FastAPI):
    """Manage application lifecycle"""
    # Startup sequence
    log.info("Initializing application...")
    
    configure_observability()
    await repository_instance.establish_connection()
    broker_instance.initialize()
    await worker_instance.begin()
    
    log.info("Application ready")
    
    yield
    
    # Shutdown sequence
    log.info("Shutting down application...")
    
    await worker_instance.halt()
    await repository_instance.close_connection()
    broker_instance.shutdown()
    
    log.info("Application stopped")


# Initialize FastAPI
application = FastAPI(
    title="Event-Driven Booking API",
    description="Booking system with transactional outbox pattern for reliable event delivery",
    version="1.0.0",
    lifespan=application_lifecycle,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Apply telemetry
apply_fastapi_instrumentation(application)


@application.get("/", tags=["Info"])
async def root_endpoint():
    """Root endpoint information"""
    return {
        "service": "Event-Driven Booking API",
        "documentation": "/api/docs",
        "health": "/health",
        "metrics": "/metrics"
    }


@application.get("/health", response_model=SystemHealth, tags=["Monitoring"])
async def health_endpoint():
    """System health check"""
    db_healthy = await repository_instance.verify_health()
    kafka_healthy = broker_instance.verify_connectivity()
    
    overall_status = "healthy" if (db_healthy and kafka_healthy) else "degraded"
    
    return SystemHealth(
        overall=overall_status,
        database_status="operational" if db_healthy else "unavailable",
        broker_status="operational" if kafka_healthy else "unavailable",
        checked_at=datetime.utcnow()
    )


@application.get("/metrics", tags=["Monitoring"])
async def metrics_endpoint():
    """Prometheus metrics"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@application.post(
    "/api/bookings",
    response_model=BookingRecord,
    status_code=status.HTTP_201_CREATED,
    tags=["Bookings"]
)
async def create_booking_endpoint(request: BookingRequest):
    """
    Create a new booking
    
    Implements transactional outbox pattern:
    - Stores booking in database
    - Stores event in outbox table (same database)
    - Background worker publishes events to Kafka
    """
    try:
        # Generate booking reference
        booking_ref = f"BK-{uuid.uuid4().hex[:10].upper()}"
        
        # Create booking record
        booking = BookingRecord(
            booking_ref=booking_ref,
            client_identifier=request.client_identifier,
            booking_items=request.booking_items,
            total_cost=request.calculated_total,
            booking_status=BookingStatus.CONFIRMED,
            notes=request.notes
        )
        
        # Persist booking
        await repository_instance.persist_booking(booking)
        
        # Create outbox event
        event = EventRecord(
            event_ref=f"EV-{uuid.uuid4().hex[:10].upper()}",
            category=EventCategory.BOOKING_CONFIRMED,
            entity_id=booking_ref,
            event_data={
                "booking_ref": booking_ref,
                "client_identifier": booking.client_identifier,
                "booking_items": [item.model_dump() for item in booking.booking_items],
                "total_cost": booking.total_cost,
                "notes": booking.notes,
                "timestamp_created": booking.timestamp_created.isoformat()
            }
        )
        
        # Store event in outbox
        await repository_instance.store_event(event)
        
        bookings_counter.inc()
        
        log.info(f"Booking {booking_ref} created with event {event.event_ref}")
        
        return booking
    
    except ValueError as error:
        log.error(f"Validation error: {error}")
        booking_errors_counter.inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error)
        )
    except Exception as error:
        log.error(f"Booking creation error: {error}")
        booking_errors_counter.inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create booking"
        )


@application.get(
    "/api/bookings/{booking_ref}",
    response_model=BookingRecord,
    tags=["Bookings"]
)
async def get_booking_endpoint(booking_ref: str):
    """Retrieve a booking by reference"""
    booking = await repository_instance.fetch_booking(booking_ref)
    
    if not booking:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Booking {booking_ref} not found"
        )
    
    return booking


@application.get(
    "/api/bookings",
    response_model=List[BookingRecord],
    tags=["Bookings"]
)
async def list_bookings_endpoint(limit: int = 100):
    """List all bookings"""
    if limit > 500:
        limit = 500
    
    bookings = await repository_instance.fetch_all_bookings(max_results=limit)
    return bookings


@application.put(
    "/api/bookings/{booking_ref}/cancel",
    response_model=BookingRecord,
    tags=["Bookings"]
)
async def cancel_booking_endpoint(booking_ref: str):
    """
    Cancel a booking
    
    Demonstrates publishing different event types via outbox pattern
    """
    try:
        # Fetch existing booking
        booking = await repository_instance.fetch_booking(booking_ref)
        
        if not booking:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Booking {booking_ref} not found"
            )
        
        # Update status
        await repository_instance.modify_booking_status(
            booking_ref,
            BookingStatus.CANCELLED.value
        )
        
        # Create cancellation event
        event = EventRecord(
            event_ref=f"EV-{uuid.uuid4().hex[:10].upper()}",
            category=EventCategory.BOOKING_CANCELLED,
            entity_id=booking_ref,
            event_data={
                "booking_ref": booking_ref,
                "client_identifier": booking.client_identifier,
                "cancellation_timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Store in outbox
        await repository_instance.store_event(event)
        
        log.info(f"Booking {booking_ref} cancelled with event {event.event_ref}")
        
        # Fetch updated booking
        updated_booking = await repository_instance.fetch_booking(booking_ref)
        return updated_booking
    
    except HTTPException:
        raise
    except Exception as error:
        log.error(f"Cancellation error: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel booking"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(application, host="0.0.0.0", port=8000)
