"""
Kafka message broker integration
"""
import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from src.config.settings import get_configuration

log = logging.getLogger(__name__)


class MessageBroker:
    """Kafka producer wrapper"""
    
    def __init__(self):
        self._producer: Optional[Producer] = None
        self._config = get_configuration()
        self._broker_config = {
            'bootstrap.servers': self._config.kafka_brokers,
            'client.id': 'outbox-publisher',
            'acks': 'all',
            'retries': 3
        }
    
    def initialize(self):
        """Initialize Kafka producer"""
        try:
            self._producer = Producer(self._broker_config)
            log.info("Kafka producer initialized")
            self._ensure_topic_exists()
        except Exception as error:
            log.error(f"Failed to initialize Kafka: {error}")
            raise
    
    def _ensure_topic_exists(self):
        """Create topic if not exists"""
        try:
            admin = AdminClient({'bootstrap.servers': self._config.kafka_brokers})
            existing_topics = admin.list_topics(timeout=10).topics
            
            if self._config.event_topic_name not in existing_topics:
                new_topic = NewTopic(
                    self._config.event_topic_name,
                    num_partitions=3,
                    replication_factor=1
                )
                futures = admin.create_topics([new_topic])
                
                for topic_name, future in futures.items():
                    try:
                        future.result()
                        log.info(f"Topic {topic_name} created")
                    except Exception as e:
                        log.warning(f"Topic creation issue: {e}")
            else:
                log.info(f"Topic {self._config.event_topic_name} exists")
        except Exception as error:
            log.error(f"Topic check failed: {error}")
    
    def shutdown(self):
        """Shutdown producer"""
        if self._producer:
            self._producer.flush(timeout=10)
            log.info("Kafka producer shut down")
    
    def verify_connectivity(self) -> bool:
        """Check Kafka connectivity"""
        try:
            admin = AdminClient({'bootstrap.servers': self._config.kafka_brokers})
            admin.list_topics(timeout=5)
            return True
        except Exception as error:
            log.error(f"Kafka health check failed: {error}")
            return False
    
    def dispatch_message(self, event_category: str, event_ref: str, data: Dict[str, Any]) -> bool:
        """Dispatch message to Kafka"""
        try:
            message_payload = {
                "event_reference": event_ref,
                "event_category": event_category,
                "data": data,
                "dispatched_at": data.get("timestamp_created", "")
            }
            
            self._producer.produce(
                topic=self._config.event_topic_name,
                key=event_ref.encode('utf-8'),
                value=json.dumps(message_payload).encode('utf-8'),
                on_delivery=self._handle_delivery
            )
            
            self._producer.poll(0)
            log.info(f"Message dispatched: {event_ref} to {self._config.event_topic_name}")
            return True
            
        except Exception as error:
            log.error(f"Failed to dispatch message {event_ref}: {error}")
            return False
    
    def _handle_delivery(self, err, msg):
        """Callback for delivery confirmation"""
        if err:
            log.error(f"Delivery error: {err}")
        else:
            log.debug(f"Message delivered to {msg.topic()} partition {msg.partition()}")


# Singleton instance
broker_instance = MessageBroker()
