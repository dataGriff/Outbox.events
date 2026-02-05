# Outbox.events

> A production-ready event-based application demonstrating the Transactional Outbox Pattern with MongoDB, Kafka, and OpenTelemetry observability.

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green.svg)](https://fastapi.tiangolo.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green.svg)](https://www.mongodb.com/)
[![Kafka](https://img.shields.io/badge/Kafka-7.5-black.svg)](https://kafka.apache.org/)

## 🎯 Overview

This repository showcases a complete event-driven architecture implementation using:

- **Transactional Outbox Pattern** for reliable event publishing
- **MongoDB** for data persistence
- **Apache Kafka** for event streaming
- **OpenTelemetry** for distributed tracing
- **Prometheus & Grafana** for metrics and visualization
- **Docker Compose** for easy local development

## ✨ Features

- ✅ **Reliable Event Publishing**: Transactional outbox pattern ensures no event loss
- ✅ **REST API**: OpenAPI-documented endpoints for business operations
- ✅ **Event Streaming**: AsyncAPI-documented event schemas
- ✅ **Distributed Tracing**: Full request tracing with OpenTelemetry and Jaeger
- ✅ **Metrics & Monitoring**: Prometheus metrics with Grafana dashboards
- ✅ **Dockerized**: Complete stack runs with single command
- ✅ **Production-Ready**: Proper error handling, logging, and health checks

## 🏗️ Architecture

The application implements the **Transactional Outbox Pattern**:

1. Business operations write data AND events to MongoDB in a single transaction
2. Background processor polls the outbox table for pending events
3. Events are reliably published to Kafka
4. Full observability with traces, metrics, and logs

For detailed architecture documentation, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 🚀 Quick Start

### Prerequisites

- Docker Desktop or Docker Engine with Docker Compose
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/dataGriff/Outbox.events.git
   cd Outbox.events
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to be ready** (takes ~30 seconds)
   ```bash
   docker-compose logs -f app
   ```

4. **Verify health**
   ```bash
   curl http://localhost:8000/health
   ```

That's it! All services are now running.

## 📊 Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **REST API (Swagger)** | http://localhost:8000/api/docs | - |
| **REST API (ReDoc)** | http://localhost:8000/api/redoc | - |
| **Health Check** | http://localhost:8000/health | - |
| **Metrics** | http://localhost:8000/metrics | - |
| **Grafana** | http://localhost:3000 | admin/admin |
| **Prometheus** | http://localhost:9090 | - |
| **Jaeger Tracing** | http://localhost:16686 | - |
| **Kafka UI** | http://localhost:8080 | - |
| **MongoDB** | mongodb://localhost:27017 | admin/password123 |

## 🎮 Usage Examples

### Create a Booking (REST API)

```bash
curl -X POST http://localhost:8000/api/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "client_identifier": "CLIENT-001",
    "booking_items": [
      {
        "item_identifier": "ITEM-001",
        "item_quantity": 2,
        "unit_cost": 75.50
      }
    ],
    "notes": "Conference room booking"
  }'
```

### Get Booking by Reference

```bash
curl http://localhost:8000/api/bookings/BK-1234567890
```

### List All Bookings

```bash
curl http://localhost:8000/api/bookings
```

### Cancel a Booking

```bash
curl -X PUT http://localhost:8000/api/bookings/BK-1234567890/cancel
```

### View Events in Kafka

Navigate to Kafka UI at http://localhost:8080 and browse the `business-events` topic to see published events.

## 📈 Monitoring & Observability

### View Traces in Jaeger

1. Open http://localhost:16686
2. Select service: `outbox-event-service`
3. Click "Find Traces"
4. Explore distributed traces across operations

### View Metrics in Grafana

1. Open http://localhost:3000 (login: admin/admin)
2. Navigate to Dashboards
3. Open "Outbox Events Dashboard"
4. View real-time metrics:
   - Orders created rate
   - Failed orders
   - Events published by type
   - Request duration percentiles

### Prometheus Queries

Access Prometheus at http://localhost:9090 and try queries:

```promql
# Orders created per second
rate(orders_created_total[5m])

# 95th percentile request duration
histogram_quantile(0.95, rate(request_duration_seconds_bucket[5m]))

# Events published by type
rate(events_published_total[5m])
```

## 🔍 How the Outbox Pattern Works

### Problem: Dual Write Problem

Writing to a database and publishing to a message broker are two separate operations. If one fails, you get inconsistent state.

### Solution: Transactional Outbox

1. **Single Transaction**: Write business data and event to the same database
   ```python
   # Store booking and event atomically
   await repository.persist_booking(booking)
   await repository.store_event(event)
   ```

2. **Background Worker**: Polls outbox table and publishes events
   ```python
   # Separate background process
   pending = await repository.retrieve_pending_events()
   for event in pending:
       broker.dispatch_message(event)
       await repository.mark_as_dispatched(event.event_ref)
   ```

3. **Benefits**:
   - ✅ Atomic writes (all or nothing)
   - ✅ Guaranteed event delivery
   - ✅ No distributed transactions needed
   - ✅ Resilient to failures

## 📚 API Documentation

### REST API (OpenAPI)

The application automatically generates OpenAPI documentation:

- **Interactive Swagger UI**: http://localhost:8000/api/docs
- **Alternative ReDoc**: http://localhost:8000/api/redoc
- **OpenAPI JSON**: http://localhost:8000/api/openapi.json

### Event Streaming API (AsyncAPI)

AsyncAPI specification for Kafka events: [docs/asyncapi.yaml](docs/asyncapi.yaml)

View it with:
- [AsyncAPI Studio](https://studio.asyncapi.com/)
- AsyncAPI Generator: `npm install -g @asyncapi/generator && ag docs/asyncapi.yaml @asyncapi/html-template`

## 🧪 Development

### Project Structure

```
Outbox.events/
├── docker-compose.yml          # Service orchestration
├── requirements.txt            # Python dependencies
├── src/
│   ├── app/
│   │   ├── main.py            # FastAPI application
│   │   ├── models.py          # Data models
│   │   ├── database.py        # MongoDB service
│   │   ├── kafka_service.py   # Kafka producer
│   │   ├── outbox_processor.py # Background event processor
│   │   └── telemetry.py       # OpenTelemetry setup
│   └── config/
│       └── settings.py        # Configuration
├── docker/
│   └── Dockerfile             # Application container
├── docs/
│   ├── ARCHITECTURE.md        # Architecture documentation
│   └── asyncapi.yaml          # AsyncAPI specification
├── grafana/                   # Grafana configuration
│   ├── provisioning/
│   └── dashboards/
└── prometheus/                # Prometheus configuration
    └── prometheus.yml
```

### Local Development

Run the application locally for development:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MONGODB_URI="mongodb://admin:password123@localhost:27017/"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"

# Start external services
docker-compose up -d mongodb kafka zookeeper

# Run application
uvicorn src.app.main:app --reload
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f app

# MongoDB logs
docker-compose logs -f mongodb

# Kafka logs
docker-compose logs -f kafka
```

## 🛠️ Configuration

Configuration is done via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URI` | `mongodb://admin:password123@mongodb:27017/` | MongoDB connection string |
| `MONGODB_DATABASE` | `outbox_db` | Database name |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka brokers |
| `KAFKA_TOPIC` | `business-events` | Kafka topic name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | OpenTelemetry endpoint |
| `OTEL_SERVICE_NAME` | `outbox-event-service` | Service name for tracing |
| `LOG_LEVEL` | `INFO` | Logging level |

## 🧹 Cleanup

Stop and remove all containers and volumes:

```bash
docker-compose down -v
```

## 📖 Additional Resources

- [Architecture Documentation](docs/ARCHITECTURE.md)
- [AsyncAPI Specification](docs/asyncapi.yaml)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [OpenTelemetry](https://opentelemetry.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## 📝 License

This project is provided as-is for educational and demonstration purposes.

## 🎓 Learning Resources

This project demonstrates:

1. **Event-Driven Architecture**: Decoupled services communicating via events
2. **Outbox Pattern**: Reliable event publishing without distributed transactions
3. **Observability**: Complete monitoring, tracing, and metrics
4. **API Documentation**: OpenAPI and AsyncAPI specifications
5. **Microservices**: Best practices for building distributed systems
6. **Docker**: Containerization and orchestration

## 🐛 Troubleshooting

### Services won't start

```bash
# Check if ports are already in use
docker-compose down
docker-compose up -d
```

### Can't connect to MongoDB

```bash
# Check MongoDB health
docker-compose logs mongodb
curl http://localhost:8000/health
```

### Kafka events not publishing

```bash
# Check Kafka logs
docker-compose logs kafka

# Check outbox processor
docker-compose logs app | grep "outbox"

# View Kafka UI
open http://localhost:8080
```

### Traces not showing in Jaeger

```bash
# Check Jaeger logs
docker-compose logs jaeger

# Verify OTLP endpoint
curl http://localhost:4318/v1/traces
```

## 📬 Contact

For questions or feedback, please open an issue on GitHub.

---

**Built with ❤️ to demonstrate production-ready event-driven architecture**