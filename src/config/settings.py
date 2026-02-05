"""
Application configuration using Pydantic settings
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class AppConfiguration(BaseSettings):
    """Central configuration for the application"""
    
    # Database configuration
    mongo_connection_string: str = "mongodb://admin:password123@localhost:27017/"
    mongo_db_name: str = "outbox_db"
    
    # Message broker configuration
    kafka_brokers: str = "localhost:9092"
    event_topic_name: str = "business-events"
    
    # Observability configuration
    otlp_endpoint: str = "http://localhost:4318"
    service_identifier: str = "outbox-event-service"
    
    # Application settings
    logging_level: str = "INFO"
    outbox_poll_seconds: int = 5
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_configuration() -> AppConfiguration:
    """Get cached configuration instance"""
    return AppConfiguration()
