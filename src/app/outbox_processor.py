"""
Background worker for processing outbox events
"""
import asyncio
import logging
from typing import Optional
from src.app.database import repository_instance
from src.app.kafka_service import broker_instance

log = logging.getLogger(__name__)


class OutboxWorker:
    """Background worker for event dispatch"""
    
    def __init__(self, interval_seconds: int = 5):
        self._interval = interval_seconds
        self._is_running = False
        self._worker_task: Optional[asyncio.Task] = None
    
    async def begin(self):
        """Start the worker"""
        self._is_running = True
        self._worker_task = asyncio.create_task(self._execution_loop())
        log.info(f"Outbox worker started (interval: {self._interval}s)")
    
    async def halt(self):
        """Stop the worker"""
        self._is_running = False
        if self._worker_task:
            await self._worker_task
        log.info("Outbox worker halted")
    
    async def _execution_loop(self):
        """Main worker loop"""
        while self._is_running:
            try:
                await self._process_awaiting_events()
            except Exception as error:
                log.error(f"Worker execution error: {error}")
            
            await asyncio.sleep(self._interval)
    
    async def _process_awaiting_events(self):
        """Process events awaiting dispatch"""
        pending_events = await repository_instance.retrieve_pending_events(batch_size=50)
        
        if not pending_events:
            log.debug("No pending events")
            return
        
        log.info(f"Processing {len(pending_events)} events from outbox")
        
        for event in pending_events:
            try:
                # Dispatch to Kafka
                dispatch_successful = broker_instance.dispatch_message(
                    event_category=event.category.value,
                    event_ref=event.event_ref,
                    data=event.event_data
                )
                
                if dispatch_successful:
                    await repository_instance.mark_as_dispatched(event.event_ref)
                    log.info(f"Event {event.event_ref} processed successfully")
                else:
                    await repository_instance.mark_as_error(event.event_ref)
                    log.error(f"Event {event.event_ref} dispatch failed")
            
            except Exception as error:
                log.error(f"Error processing event {event.event_ref}: {error}")
                await repository_instance.mark_as_error(event.event_ref)


# Singleton worker instance
worker_instance = OutboxWorker()
