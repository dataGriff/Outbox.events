"""
Configuration settings for the Outbox Events application.
"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # MongoDB settings
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://admin:password123@localhost:27017/")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "outbox_db")
    
    # Kafka settings
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "business-events")
    
    # OpenTelemetry settings
    otel_exporter_otlp_endpoint: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    otel_service_name: str = os.getenv("OTEL_SERVICE_NAME", "outbox-event-service")
    
    # Application settings
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    class Config:
        env_file = ".env"


settings = Settings()
