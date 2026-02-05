"""
Main FastAPI application with Outbox Pattern implementation
"""
import logging
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from src.config.settings import settings
from src.app.database import db_service
from src.app.kafka_service import kafka_service
from src.app.outbox_processor import outbox_processor
from src.app.telemetry import setup_telemetry, instrument_fastapi
from src.app.models import (
    Order, OrderCreateRequest, OrderResponse, OutboxEvent,
    EventType, HealthResponse
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
order_created_counter = Counter('orders_created_total', 'Total number of orders created')
order_failed_counter = Counter('orders_failed_total', 'Total number of failed orders')
events_published_counter = Counter('events_published_total', 'Total number of events published', ['event_type'])
request_duration = Histogram('request_duration_seconds', 'Request duration', ['method', 'endpoint'])


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Outbox Events application...")
    
    # Setup telemetry
    setup_telemetry()
    
    # Connect to MongoDB
    await db_service.connect()
    
    # Connect to Kafka
    kafka_service.connect()
    
    # Start outbox processor
    await outbox_processor.start()
    
    logger.info("Application started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    # Stop outbox processor
    await outbox_processor.stop()
    
    # Disconnect from services
    await db_service.disconnect()
    kafka_service.disconnect()
    
    logger.info("Application shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="Outbox Events API",
    description="Event-based application using MongoDB and Kafka with Outbox Pattern",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Instrument FastAPI with OpenTelemetry
instrument_fastapi(app)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Outbox Events API",
        "docs": "/api/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    mongodb_healthy = await db_service.health_check()
    kafka_healthy = kafka_service.health_check()
    
    overall_status = "healthy" if (mongodb_healthy and kafka_healthy) else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        mongodb="healthy" if mongodb_healthy else "unhealthy",
        kafka="healthy" if kafka_healthy else "unhealthy",
        timestamp=datetime.utcnow()
    )


@app.get("/metrics", tags=["Metrics"])
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/api/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED, tags=["Orders"])
async def create_order(order_request: OrderCreateRequest):
    """
    Create a new order.
    
    This endpoint demonstrates the transactional outbox pattern:
    1. Creates an order in MongoDB
    2. Saves an event to the outbox table in the same database
    3. Background processor publishes events to Kafka asynchronously
    """
    try:
        # Generate unique order ID
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        
        # Create order
        order = Order(
            order_id=order_id,
            customer_id=order_request.customer_id,
            items=order_request.items,
            total_amount=order_request.total_amount,
            status="pending"
        )
        
        # Save order to database
        await db_service.create_order(order)
        
        # Create outbox event
        event = OutboxEvent(
            event_id=f"EVT-{uuid.uuid4().hex[:8].upper()}",
            event_type=EventType.ORDER_CREATED,
            aggregate_id=order_id,
            payload={
                "order_id": order_id,
                "customer_id": order.customer_id,
                "items": order.items,
                "total_amount": order.total_amount,
                "status": order.status,
                "created_at": order.created_at.isoformat()
            }
        )
        
        # Save event to outbox (same database, ensuring atomicity)
        await db_service.save_outbox_event(event)
        
        # Update metrics
        order_created_counter.inc()
        
        logger.info(f"Order {order_id} created successfully with event {event.event_id}")
        
        return OrderResponse(
            order_id=order.order_id,
            customer_id=order.customer_id,
            items=order.items,
            total_amount=order.total_amount,
            status=order.status,
            created_at=order.created_at,
            updated_at=order.updated_at
        )
    
    except Exception as e:
        logger.error(f"Failed to create order: {e}")
        order_failed_counter.inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create order: {str(e)}"
        )


@app.get("/api/orders/{order_id}", response_model=OrderResponse, tags=["Orders"])
async def get_order(order_id: str):
    """Get an order by ID"""
    order = await db_service.get_order(order_id)
    
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order {order_id} not found"
        )
    
    return OrderResponse(
        order_id=order.order_id,
        customer_id=order.customer_id,
        items=order.items,
        total_amount=order.total_amount,
        status=order.status,
        created_at=order.created_at,
        updated_at=order.updated_at
    )


@app.get("/api/orders", response_model=List[OrderResponse], tags=["Orders"])
async def list_orders(limit: int = 100):
    """List all orders"""
    orders = await db_service.list_orders(limit=limit)
    
    return [
        OrderResponse(
            order_id=order.order_id,
            customer_id=order.customer_id,
            items=order.items,
            total_amount=order.total_amount,
            status=order.status,
            created_at=order.created_at,
            updated_at=order.updated_at
        )
        for order in orders
    ]


@app.put("/api/orders/{order_id}/cancel", response_model=OrderResponse, tags=["Orders"])
async def cancel_order(order_id: str):
    """
    Cancel an order.
    
    This demonstrates publishing a different event type through the outbox pattern.
    """
    try:
        # Get existing order
        order = await db_service.get_order(order_id)
        
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        
        # Update order status
        await db_service.update_order_status(order_id, "cancelled")
        
        # Create outbox event for cancellation
        event = OutboxEvent(
            event_id=f"EVT-{uuid.uuid4().hex[:8].upper()}",
            event_type=EventType.ORDER_CANCELLED,
            aggregate_id=order_id,
            payload={
                "order_id": order_id,
                "customer_id": order.customer_id,
                "cancelled_at": datetime.utcnow().isoformat()
            }
        )
        
        # Save event to outbox
        await db_service.save_outbox_event(event)
        
        logger.info(f"Order {order_id} cancelled with event {event.event_id}")
        
        # Get updated order
        updated_order = await db_service.get_order(order_id)
        
        return OrderResponse(
            order_id=updated_order.order_id,
            customer_id=updated_order.customer_id,
            items=updated_order.items,
            total_amount=updated_order.total_amount,
            status=updated_order.status,
            created_at=updated_order.created_at,
            updated_at=updated_order.updated_at
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel order: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel order: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
