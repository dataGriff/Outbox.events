"""
MongoDB persistence layer
"""
import logging
from typing import List, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from src.config.settings import get_configuration
from src.app.models import BookingRecord, EventRecord, OutboxState

log = logging.getLogger(__name__)


class MongoRepository:
    """Repository pattern for MongoDB operations"""
    
    def __init__(self):
        self._client: Optional[AsyncIOMotorClient] = None
        self._database: Optional[AsyncIOMotorDatabase] = None
        self._config = get_configuration()
    
    async def establish_connection(self):
        """Establish connection to MongoDB"""
        try:
            self._client = AsyncIOMotorClient(
                self._config.mongo_connection_string,
                serverSelectionTimeoutMS=5000
            )
            self._database = self._client[self._config.mongo_db_name]
            
            # Verify connection
            await self._client.admin.command('ping')
            log.info("MongoDB connection established successfully")
            
            # Setup indexes
            await self._setup_indexes()
            
        except ConnectionFailure as error:
            log.error(f"MongoDB connection failed: {error}")
            raise
    
    async def close_connection(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            log.info("MongoDB connection closed")
    
    async def _setup_indexes(self):
        """Create required indexes"""
        bookings_collection = self._database.bookings
        outbox_collection = self._database.event_outbox
        
        # Bookings indexes
        await bookings_collection.create_index("booking_ref", unique=True)
        await bookings_collection.create_index("client_identifier")
        await bookings_collection.create_index([("timestamp_created", -1)])
        
        # Outbox indexes
        await outbox_collection.create_index("event_ref", unique=True)
        await outbox_collection.create_index([("current_state", 1), ("timestamp_created", 1)])
        await outbox_collection.create_index("entity_id")
        
        log.info("Database indexes configured")
    
    async def verify_health(self) -> bool:
        """Check database health"""
        try:
            await self._client.admin.command('ping')
            return True
        except Exception as error:
            log.error(f"Health check failed: {error}")
            return False
    
    # Booking operations
    async def persist_booking(self, booking: BookingRecord) -> str:
        """Store a booking record"""
        try:
            booking_dict = booking.model_dump()
            await self._database.bookings.insert_one(booking_dict)
            log.info(f"Persisted booking: {booking.booking_ref}")
            return booking.booking_ref
        except DuplicateKeyError:
            log.warning(f"Booking {booking.booking_ref} already exists")
            raise ValueError(f"Booking {booking.booking_ref} already exists")
    
    async def fetch_booking(self, booking_ref: str) -> Optional[BookingRecord]:
        """Retrieve a booking by reference"""
        doc = await self._database.bookings.find_one({"booking_ref": booking_ref})
        if doc:
            doc.pop('_id', None)
            return BookingRecord(**doc)
        return None
    
    async def fetch_all_bookings(self, max_results: int = 100) -> List[BookingRecord]:
        """Retrieve all bookings"""
        cursor = self._database.bookings.find().limit(max_results).sort("timestamp_created", -1)
        bookings = []
        async for doc in cursor:
            doc.pop('_id', None)
            bookings.append(BookingRecord(**doc))
        return bookings
    
    async def modify_booking_status(self, booking_ref: str, new_status: str) -> bool:
        """Update booking status"""
        result = await self._database.bookings.update_one(
            {"booking_ref": booking_ref},
            {
                "$set": {
                    "booking_status": new_status,
                    "timestamp_modified": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0
    
    # Outbox operations
    async def store_event(self, event: EventRecord) -> str:
        """Store event in outbox"""
        try:
            event_dict = event.model_dump()
            await self._database.event_outbox.insert_one(event_dict)
            log.info(f"Event stored in outbox: {event.event_ref}")
            return event.event_ref
        except DuplicateKeyError:
            log.warning(f"Event {event.event_ref} already exists")
            raise ValueError(f"Event {event.event_ref} already exists")
    
    async def retrieve_pending_events(self, batch_size: int = 50) -> List[EventRecord]:
        """Get events awaiting dispatch"""
        cursor = self._database.event_outbox.find(
            {"current_state": OutboxState.AWAITING.value}
        ).limit(batch_size).sort("timestamp_created", 1)
        
        events = []
        async for doc in cursor:
            doc.pop('_id', None)
            events.append(EventRecord(**doc))
        return events
    
    async def mark_as_dispatched(self, event_ref: str) -> bool:
        """Mark event as successfully dispatched"""
        result = await self._database.event_outbox.update_one(
            {"event_ref": event_ref},
            {
                "$set": {
                    "current_state": OutboxState.DISPATCHED.value,
                    "timestamp_dispatched": datetime.utcnow()
                }
            }
        )
        success = result.modified_count > 0
        if success:
            log.info(f"Event {event_ref} marked as dispatched")
        return success
    
    async def mark_as_error(self, event_ref: str) -> bool:
        """Mark event as failed"""
        result = await self._database.event_outbox.update_one(
            {"event_ref": event_ref},
            {
                "$set": {"current_state": OutboxState.ERROR.value},
                "$inc": {"attempt_count": 1}
            }
        )
        return result.modified_count > 0


# Singleton instance
repository_instance = MongoRepository()
