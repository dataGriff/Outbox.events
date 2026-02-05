"""
OpenTelemetry instrumentation setup
"""
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.sdk.resources import Resource
from src.config.settings import get_configuration

log = logging.getLogger(__name__)


def configure_observability():
    """Configure OpenTelemetry tracing"""
    try:
        config = get_configuration()
        
        # Define service resource
        service_resource = Resource.create(
            {"service.name": config.service_identifier}
        )
        
        # Initialize tracer provider
        provider = TracerProvider(resource=service_resource)
        
        # Configure OTLP exporter
        span_exporter = OTLPSpanExporter(
            endpoint=f"{config.otlp_endpoint}/v1/traces"
        )
        
        # Attach span processor
        provider.add_span_processor(BatchSpanProcessor(span_exporter))
        
        # Register provider
        trace.set_tracer_provider(provider)
        
        # Instrument PyMongo
        PymongoInstrumentor().instrument()
        
        log.info("OpenTelemetry configuration complete")
        
    except Exception as error:
        log.error(f"Telemetry setup error: {error}")


def apply_fastapi_instrumentation(application):
    """Apply instrumentation to FastAPI app"""
    try:
        FastAPIInstrumentor.instrument_app(application)
        log.info("FastAPI telemetry applied")
    except Exception as error:
        log.error(f"FastAPI instrumentation error: {error}")
