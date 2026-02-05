"""
Database service for MongoDB operations
"""
import logging
from typing import Optional, List
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure
from src.config.settings import settings
from src.app.models import Order, OutboxEvent, EventStatus

logger = logging.getLogger(__name__)


class DatabaseService:
    """MongoDB database service"""
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(settings.mongodb_uri)
            self.db = self.client[settings.mongodb_database]
            # Test connection
            await self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            
            # Create indexes
            await self._create_indexes()
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    async def _create_indexes(self):
        """Create necessary indexes"""
        # Orders collection indexes
        await self.db.orders.create_index("order_id", unique=True)
        await self.db.orders.create_index("customer_id")
        
        # Outbox collection indexes
        await self.db.outbox.create_index("event_id", unique=True)
        await self.db.outbox.create_index([("status", 1), ("created_at", 1)])
        
        logger.info("Database indexes created")
    
    async def health_check(self) -> bool:
        """Check if database is healthy"""
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    # Order operations
    async def create_order(self, order: Order) -> str:
        """Create a new order"""
        order_dict = order.model_dump()
        result = await self.db.orders.insert_one(order_dict)
        logger.info(f"Created order: {order.order_id}")
        return order.order_id
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """Get an order by ID"""
        order_dict = await self.db.orders.find_one({"order_id": order_id})
        if order_dict:
            order_dict.pop('_id', None)
            return Order(**order_dict)
        return None
    
    async def list_orders(self, limit: int = 100) -> List[Order]:
        """List all orders"""
        cursor = self.db.orders.find().limit(limit).sort("created_at", -1)
        orders = []
        async for order_dict in cursor:
            order_dict.pop('_id', None)
            orders.append(Order(**order_dict))
        return orders
    
    async def update_order_status(self, order_id: str, status: str) -> bool:
        """Update order status"""
        result = await self.db.orders.update_one(
            {"order_id": order_id},
            {"$set": {"status": status, "updated_at": datetime.utcnow()}}
        )
        return result.modified_count > 0
    
    # Outbox operations
    async def save_outbox_event(self, event: OutboxEvent) -> str:
        """Save an event to the outbox"""
        event_dict = event.model_dump()
        await self.db.outbox.insert_one(event_dict)
        logger.info(f"Saved outbox event: {event.event_id}")
        return event.event_id
    
    async def get_pending_events(self, limit: int = 100) -> List[OutboxEvent]:
        """Get pending events from outbox"""
        cursor = self.db.outbox.find(
            {"status": EventStatus.PENDING.value}
        ).limit(limit).sort("created_at", 1)
        
        events = []
        async for event_dict in cursor:
            event_dict.pop('_id', None)
            events.append(OutboxEvent(**event_dict))
        return events
    
    async def mark_event_published(self, event_id: str) -> bool:
        """Mark an event as published"""
        result = await self.db.outbox.update_one(
            {"event_id": event_id},
            {
                "$set": {
                    "status": EventStatus.PUBLISHED.value,
                    "published_at": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0
    
    async def mark_event_failed(self, event_id: str) -> bool:
        """Mark an event as failed"""
        result = await self.db.outbox.update_one(
            {"event_id": event_id},
            {
                "$set": {"status": EventStatus.FAILED.value},
                "$inc": {"retry_count": 1}
            }
        )
        return result.modified_count > 0


# Global database service instance
db_service = DatabaseService()
