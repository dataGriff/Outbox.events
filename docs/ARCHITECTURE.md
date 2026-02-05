# Outbox Events - Architecture Documentation

## Overview

This project demonstrates a production-ready event-based application using the **Transactional Outbox Pattern** with MongoDB and Kafka. The application ensures reliable event publishing with distributed tracing and observability using OpenTelemetry, Prometheus, and Grafana.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client / API Consumer                       │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP/REST
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI Application                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                 OpenTelemetry Instrumentation              │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌────────────────┐              ┌─────────────────────────┐  │
│  │ Business Logic │──────────────│  Outbox Worker          │  │
│  │ (Bookings API) │              │  (Background Process)   │  │
│  └────────┬───────┘              └──────────┬──────────────┘  │
└───────────┼─────────────────────────────────┼─────────────────┘
            │                                  │
            ▼                                  ▼
┌───────────────────────────┐      ┌──────────────────────────┐
│         MongoDB           │      │        Kafka Broker       │
│  ┌────────────────────┐  │      │  ┌───────────────────┐   │
│  │Bookings Collection │  │      │  │  business-events  │   │
│  └────────────────────┘  │      │  │     (topic)       │   │
│  ┌────────────────────┐  │      │  └───────────────────┘   │
│  │Event Outbox Table  │  │      └──────────────────────────┘
│  │  (Event Queue)     │  │                  │
│  └────────────────────┘  │                  │
└───────────────────────────┘                  ▼
            │                         Event Consumers
            │                      (Other Microservices)
            ▼
┌────────────────────────────────────────────────────────────────┐
│                    Observability Stack                          │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │   Jaeger     │  │  Prometheus  │  │      Grafana       │  │
│  │  (Traces)    │  │  (Metrics)   │  │  (Visualization)   │  │
│  └──────────────┘  └──────────────┘  └────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Transactional Outbox Pattern

The outbox pattern ensures reliable event publishing without distributed transactions:

1. **Write Phase**: Business operation and event are written to MongoDB in a single transaction
   - Booking is inserted into `bookings` collection
   - Event is inserted into `event_outbox` collection
   - Both operations succeed or fail together (atomicity)

2. **Publish Phase**: Background worker polls the outbox table
   - Retrieves awaiting events from `event_outbox` collection
   - Publishes events to Kafka
   - Marks events as dispatched on success
   - Retries failed events

3. **Benefits**:
   - Guaranteed event delivery (at-least-once semantics)
   - No dual-write problem
   - Resilient to service failures
   - Decouples business logic from event publishing

### 2. Technology Stack

- **FastAPI**: Modern Python web framework with automatic OpenAPI documentation
- **MongoDB**: Document database for business data and outbox storage
- **Kafka**: Distributed event streaming platform
- **OpenTelemetry**: Distributed tracing and observability
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Metrics visualization and dashboarding
- **Jaeger**: Distributed tracing UI
- **Docker Compose**: Local development orchestration

### 3. Data Flow

```
1. POST /api/bookings
   └─> Create Booking in MongoDB
       └─> Save Event to Outbox (same DB transaction)
           └─> Return Response (201 Created)

2. Outbox Worker (Background)
   └─> Poll Outbox for Awaiting Events
       └─> Publish to Kafka
           └─> Mark as Dispatched
               └─> Event Available for Consumers
```

### 4. Event Types

The application supports several business event types (defined in `EventCategory` enum):

- `booking.confirmed` - New booking created
- `booking.cancelled` - Booking cancelled
- `booking.completed` - Booking completed
- `payment.success` - Payment successfully processed
- `payment.declined` - Payment failed

## API Documentation

### REST API (OpenAPI)

The application provides a REST API documented with OpenAPI 3.0:
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **OpenAPI JSON**: http://localhost:8000/api/openapi.json

### Event Streaming API (AsyncAPI)

Event streaming is documented with AsyncAPI:
- **Specification**: `docs/asyncapi.yaml`
- View with AsyncAPI tools or online viewer

## Observability

### Metrics (Prometheus)

Custom application metrics:
- `booking_transactions_total`: Counter for bookings created
- `booking_errors_total`: Counter for failed bookings
- `domain_events_total`: Counter for dispatched events by category
- `api_latency_seconds`: Histogram for request latency

Access Prometheus: http://localhost:9090

### Visualization (Grafana)

Pre-configured dashboards showing:
- Order creation rate
- Failed order rate
- Events published by type
- Request duration percentiles

Access Grafana: http://localhost:3000 (admin/admin)

### Distributed Tracing (Jaeger)

OpenTelemetry automatically traces:
- HTTP requests through FastAPI
- MongoDB operations
- Event publishing to Kafka

Access Jaeger UI: http://localhost:16686

### Kafka Monitoring

Kafka UI for topic and message inspection:
- Topic management
- Message browsing
- Consumer group monitoring

Access Kafka UI: http://localhost:8080

## Database Schema

### Bookings Collection
```json
{
  "booking_ref": "BK-1234567890",
  "client_identifier": "CLIENT-001",
  "booking_items": [
    {
      "item_identifier": "ITEM-001",
      "item_quantity": 2,
      "unit_cost": 75.50
    }
  ],
  "total_cost": 151.00,
  "booking_status": "confirmed",
  "notes": "Conference room",
  "timestamp_created": "2024-01-01T10:00:00Z",
  "timestamp_modified": "2024-01-01T10:00:00Z"
}
```

### Event Outbox Collection
```json
{
  "event_ref": "EV-A1B2C3D4E5",
  "category": "booking.confirmed",
  "entity_id": "BK-1234567890",
  "event_data": {
    "booking_ref": "BK-1234567890",
    "client_identifier": "CLIENT-001",
    "booking_items": [...],
    "total_cost": 151.00,
    "notes": "Conference room",
    "timestamp_created": "2024-01-01T10:00:00Z"
  },
  "current_state": "awaiting",
  "timestamp_created": "2024-01-01T10:00:00Z",
  "timestamp_dispatched": null,
  "attempt_count": 0
}
```

## Security Considerations

1. **Authentication**: Currently not implemented (example project)
2. **Authorization**: Add role-based access control for production
3. **Encryption**: Use TLS for all network communication
4. **Secrets Management**: Use environment variables or secret managers
5. **Input Validation**: Pydantic models provide validation
6. **Rate Limiting**: Add for production deployments

## Scalability

The architecture supports horizontal scaling:

1. **Application Tier**: Multiple FastAPI instances behind load balancer
2. **Outbox Processor**: Multiple processors with distributed locking
3. **MongoDB**: Replica set or sharded cluster
4. **Kafka**: Multi-broker cluster with replication

## Error Handling

1. **Business Logic Errors**: Returned as HTTP errors
2. **Database Failures**: Automatic retries, circuit breakers
3. **Kafka Publishing Failures**: Events remain in outbox for retry
4. **Network Issues**: Retry logic with exponential backoff

## Monitoring & Alerts

Recommended alerts:
- High failed booking rate
- Outbox event lag (awaiting events growing)
- Kafka publishing failures
- High request latency (p95 > threshold)
- Database connection issues

## Future Enhancements

1. **Idempotency**: Add idempotency keys for operations
2. **Dead Letter Queue**: Handle permanently failed events
3. **Event Versioning**: Support for event schema evolution
4. **CQRS**: Separate read and write models
5. **Saga Pattern**: Distributed transaction coordination
6. **CDC**: Change Data Capture as alternative to outbox pattern
