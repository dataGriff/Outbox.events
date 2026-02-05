"""
OpenTelemetry configuration and instrumentation
"""
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.sdk.resources import Resource
from src.config.settings import settings

logger = logging.getLogger(__name__)


def setup_telemetry():
    """Setup OpenTelemetry instrumentation"""
    try:
        # Create resource with service name
        resource = Resource.create({"service.name": settings.otel_service_name})
        
        # Setup trace provider
        trace_provider = TracerProvider(resource=resource)
        
        # Setup OTLP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/traces"
        )
        
        # Add span processor
        trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        
        # Set global trace provider
        trace.set_tracer_provider(trace_provider)
        
        # Instrument PyMongo
        PymongoInstrumentor().instrument()
        
        logger.info("OpenTelemetry instrumentation configured")
        
    except Exception as e:
        logger.error(f"Failed to setup telemetry: {e}")


def instrument_fastapi(app):
    """Instrument FastAPI application"""
    try:
        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI instrumented with OpenTelemetry")
    except Exception as e:
        logger.error(f"Failed to instrument FastAPI: {e}")
