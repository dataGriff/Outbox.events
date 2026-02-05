"""
Kafka service for event publishing
"""
import json
import logging
from typing import Dict, Any
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from src.config.settings import settings

logger = logging.getLogger(__name__)


class KafkaService:
    """Kafka service for publishing events"""
    
    def __init__(self):
        self.producer: Optional[Producer] = None
        self.config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'client.id': 'outbox-event-publisher'
        }
    
    def connect(self):
        """Initialize Kafka producer"""
        try:
            self.producer = Producer(self.config)
            logger.info("Kafka producer initialized")
            
            # Create topic if it doesn't exist
            self._create_topic_if_not_exists(settings.kafka_topic)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def _create_topic_if_not_exists(self, topic_name: str):
        """Create Kafka topic if it doesn't exist"""
        try:
            admin_client = AdminClient({'bootstrap.servers': settings.kafka_bootstrap_servers})
            
            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if topic_name not in metadata.topics:
                topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)
                fs = admin_client.create_topics([topic])
                
                # Wait for operation to complete
                for topic, f in fs.items():
                    try:
                        f.result()
                        logger.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        logger.warning(f"Failed to create topic {topic}: {e}")
            else:
                logger.info(f"Topic {topic_name} already exists")
        except Exception as e:
            logger.error(f"Error checking/creating topic: {e}")
    
    def disconnect(self):
        """Disconnect Kafka producer"""
        if self.producer:
            self.producer.flush()
            logger.info("Kafka producer disconnected")
    
    def health_check(self) -> bool:
        """Check if Kafka is healthy"""
        try:
            admin_client = AdminClient({'bootstrap.servers': settings.kafka_bootstrap_servers})
            metadata = admin_client.list_topics(timeout=5)
            return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False
    
    def publish_event(self, event_type: str, event_id: str, payload: Dict[str, Any]) -> bool:
        """Publish an event to Kafka"""
        try:
            message = {
                "event_id": event_id,
                "event_type": event_type,
                "payload": payload,
                "timestamp": payload.get("created_at", "")
            }
            
            self.producer.produce(
                settings.kafka_topic,
                key=event_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            logger.info(f"Published event {event_id} to Kafka topic {settings.kafka_topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event {event_id}: {e}")
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Global Kafka service instance
kafka_service = KafkaService()
