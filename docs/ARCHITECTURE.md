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
│  │ Business Logic │──────────────│  Outbox Processor       │  │
│  │ (Orders API)   │              │  (Background Worker)    │  │
│  └────────┬───────┘              └──────────┬──────────────┘  │
└───────────┼─────────────────────────────────┼─────────────────┘
            │                                  │
            ▼                                  ▼
┌───────────────────────────┐      ┌──────────────────────────┐
│         MongoDB           │      │        Kafka Broker       │
│  ┌────────────────────┐  │      │  ┌───────────────────┐   │
│  │  Orders Collection │  │      │  │  business-events  │   │
│  └────────────────────┘  │      │  │     (topic)       │   │
│  ┌────────────────────┐  │      │  └───────────────────┘   │
│  │ Outbox Collection  │  │      └──────────────────────────┘
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
   - Order is inserted into `orders` collection
   - Event is inserted into `outbox` collection
   - Both operations succeed or fail together (atomicity)

2. **Publish Phase**: Background processor polls the outbox table
   - Retrieves pending events from `outbox` collection
   - Publishes events to Kafka
   - Marks events as published on success
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
1. POST /api/orders
   └─> Create Order in MongoDB
       └─> Save Event to Outbox (same DB transaction)
           └─> Return Response (201 Created)

2. Outbox Processor (Background)
   └─> Poll Outbox for Pending Events
       └─> Publish to Kafka
           └─> Mark as Published
               └─> Event Available for Consumers
```

### 4. Event Types

The application supports several business event types:

- `order.created` - New order created
- `order.updated` - Order information updated
- `order.cancelled` - Order cancelled
- `payment.processed` - Payment successfully processed
- `payment.failed` - Payment failed
- `shipment.created` - Shipment created

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
- `orders_created_total`: Counter for orders created
- `orders_failed_total`: Counter for failed orders
- `events_published_total`: Counter for published events by type
- `request_duration_seconds`: Histogram for request latency

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

### Orders Collection
```json
{
  "order_id": "ORD-12345678",
  "customer_id": "CUST-001",
  "items": [
    {
      "product_id": "PROD-001",
      "quantity": 2,
      "price": 49.99
    }
  ],
  "total_amount": 99.98,
  "status": "pending",
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-01T10:00:00Z"
}
```

### Outbox Collection
```json
{
  "event_id": "EVT-A1B2C3D4",
  "event_type": "order.created",
  "aggregate_id": "ORD-12345678",
  "payload": {
    "order_id": "ORD-12345678",
    "customer_id": "CUST-001",
    "items": [...],
    "total_amount": 99.98,
    "status": "pending",
    "created_at": "2024-01-01T10:00:00Z"
  },
  "status": "pending",
  "created_at": "2024-01-01T10:00:00Z",
  "published_at": null,
  "retry_count": 0
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
- High failed order rate
- Outbox event lag (pending events growing)
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
