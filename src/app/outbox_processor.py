"""
Outbox processor service - polls outbox table and publishes events to Kafka
"""
import asyncio
import logging
from typing import Optional
from src.app.database import db_service
from src.app.kafka_service import kafka_service

logger = logging.getLogger(__name__)


class OutboxProcessor:
    """Service to process outbox events and publish to Kafka"""
    
    def __init__(self, poll_interval: int = 5):
        self.poll_interval = poll_interval
        self.running = False
        self.task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start the outbox processor"""
        self.running = True
        self.task = asyncio.create_task(self._process_loop())
        logger.info("Outbox processor started")
    
    async def stop(self):
        """Stop the outbox processor"""
        self.running = False
        if self.task:
            await self.task
        logger.info("Outbox processor stopped")
    
    async def _process_loop(self):
        """Main processing loop"""
        while self.running:
            try:
                await self._process_pending_events()
            except Exception as e:
                logger.error(f"Error processing outbox events: {e}")
            
            await asyncio.sleep(self.poll_interval)
    
    async def _process_pending_events(self):
        """Process pending events from outbox"""
        events = await db_service.get_pending_events(limit=100)
        
        if not events:
            logger.debug("No pending events to process")
            return
        
        logger.info(f"Processing {len(events)} pending events")
        
        for event in events:
            try:
                # Publish to Kafka
                success = kafka_service.publish_event(
                    event_type=event.event_type.value,
                    event_id=event.event_id,
                    payload=event.payload
                )
                
                if success:
                    # Mark as published in database
                    await db_service.mark_event_published(event.event_id)
                    logger.info(f"Successfully processed event {event.event_id}")
                else:
                    # Mark as failed
                    await db_service.mark_event_failed(event.event_id)
                    logger.error(f"Failed to process event {event.event_id}")
            
            except Exception as e:
                logger.error(f"Error processing event {event.event_id}: {e}")
                await db_service.mark_event_failed(event.event_id)


# Global outbox processor instance
outbox_processor = OutboxProcessor()
