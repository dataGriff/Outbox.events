# Getting Started Guide

This guide will help you get the Outbox Events application up and running quickly.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Docker Desktop** (or Docker Engine + Docker Compose)
  - [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)
  - Minimum 4GB RAM allocated to Docker
  - At least 10GB free disk space

- **Git** (for cloning the repository)
  - [Install Git](https://git-scm.com/downloads)

- **curl** or **Postman** (for testing API endpoints)

## Step 1: Clone the Repository

```bash
git clone https://github.com/dataGriff/Outbox.events.git
cd Outbox.events
```

## Step 2: Start the Application

Start all services with a single command:

```bash
docker-compose up -d
```

This will start:
- MongoDB (database)
- Zookeeper (Kafka dependency)
- Kafka (event streaming)
- Kafka UI (web interface for Kafka)
- Prometheus (metrics collection)
- Grafana (metrics visualization)
- Jaeger (distributed tracing)
- Application (FastAPI service)

## Step 3: Verify Services

Wait about 30-60 seconds for all services to be ready, then check health:

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{
  "status": "healthy",
  "mongodb": "healthy",
  "kafka": "healthy",
  "timestamp": "2024-01-01T10:00:00.000000"
}
```

## Step 4: Explore the API

### Open API Documentation

Visit the interactive API documentation:
- **Swagger UI**: http://localhost:8000/api/docs

### Create Your First Booking

Using curl:
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

Or use the Swagger UI at http://localhost:8000/api/docs to try it interactively.

### View the Created Booking

Get the booking (replace BOOKING_REF with the reference from the response):
```bash
curl http://localhost:8000/api/bookings/BK-1234567890
```

List all bookings:
```bash
curl http://localhost:8000/api/bookings
```

## Step 5: See Events in Kafka

### Option 1: Kafka UI (Recommended)

1. Open http://localhost:8080
2. Click on "Topics"
3. Click on "business-events"
4. Click "Messages" to see published events

### Option 2: Check Application Logs

```bash
docker-compose logs app | grep "Published event"
```

You should see log entries showing events being published to Kafka.

## Step 6: View Traces in Jaeger

1. Open http://localhost:16686
2. In the "Service" dropdown, select `outbox-event-service`
3. Click "Find Traces"
4. Click on any trace to see the full request flow

You'll see traces showing:
- HTTP request handling
- MongoDB operations
- Event publishing to Kafka

## Step 7: View Metrics in Grafana

1. Open http://localhost:3000
2. Login with:
   - Username: `admin`
   - Password: `admin`
3. Click "Skip" if asked to change password
4. Go to Dashboards → Browse
5. Open "Outbox Events Dashboard"

The dashboard shows:
- Bookings created per second
- Failed bookings
- Events published by type
- Request duration metrics

## Step 8: Create More Bookings and See the System in Action

Run multiple booking creation requests:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8000/api/bookings \
    -H "Content-Type: application/json" \
    -d "{
      \"client_identifier\": \"CLIENT-$(printf %03d $i)\",
      \"booking_items\": [{
        \"item_identifier\": \"ITEM-001\",
        \"item_quantity\": $i,
        \"unit_cost\": 50.00
      }],
      \"notes\": \"Test booking $i\"
    }"
  echo ""
done
```

Now observe:
- **Kafka UI**: See new events appearing
- **Grafana**: See metrics increase in real-time
- **Jaeger**: See new traces being created

## Understanding the Outbox Pattern

When you create a booking, here's what happens:

1. **Booking Storage**: Booking is saved to MongoDB `bookings` collection
2. **Event Storage**: Event is saved to MongoDB `event_outbox` collection (same database, atomic)
3. **Background Worker**: Outbox worker (polling every 5 seconds) finds awaiting events
4. **Event Dispatch**: Events are dispatched to Kafka topic `business-events`
5. **Status Update**: Event status is updated to "dispatched" in outbox collection

This ensures that events are never lost, even if Kafka is temporarily unavailable!

## Exploring MongoDB Data

Connect to MongoDB to see the data:

```bash
# Connect to MongoDB container
docker exec -it outbox-mongodb mongosh -u admin -p password123

# Switch to database
use outbox_db

# View bookings
db.bookings.find().pretty()

# View outbox events
db.event_outbox.find().pretty()

# Count awaiting events
db.event_outbox.countDocuments({current_state: "awaiting"})

# Count dispatched events
db.event_outbox.countDocuments({current_state: "dispatched"})

# Exit
exit
```

## Testing Booking Cancellation

Cancel a booking to see a different event type:

```bash
curl -X PUT http://localhost:8000/api/bookings/BK-1234567890/cancel
```

Check Kafka UI to see the `booking.cancelled` event!

## Viewing Prometheus Metrics Directly

1. Open http://localhost:9090
2. Click "Graph"
3. Try these queries:

```promql
# Bookings created rate
rate(booking_transactions_total[1m])

# Total bookings created
booking_transactions_total

# Events by category
domain_events_total
```

## Next Steps

Now that you have the basics working, you can:

1. **Read the Architecture Documentation**: [docs/ARCHITECTURE.md](ARCHITECTURE.md)
2. **Explore AsyncAPI Specification**: [docs/asyncapi.yaml](asyncapi.yaml)
3. **Modify the Code**: The application uses hot-reload, so changes are reflected immediately
4. **Add New Event Types**: Extend the `EventCategory` enum in `src/app/models.py`
5. **Build Event Consumers**: Create services that subscribe to Kafka topics

## Troubleshooting

### Application won't start

Check logs:
```bash
docker-compose logs app
```

Restart services:
```bash
docker-compose down
docker-compose up -d
```

### Port conflicts

If ports 8000, 3000, 9090, etc. are already in use, modify `docker-compose.yml` to use different ports.

### Slow performance

Ensure Docker has enough resources:
- Docker Desktop → Settings → Resources
- Allocate at least 4GB RAM and 2 CPUs

### MongoDB connection issues

```bash
# Check MongoDB is running
docker-compose ps mongodb

# Check MongoDB logs
docker-compose logs mongodb

# Test connection
docker exec -it outbox-mongodb mongosh --eval "db.adminCommand('ping')"
```

### Kafka issues

```bash
# Check Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Wait longer - Kafka can take 30-60 seconds to start
```

## Stopping the Application

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v
```

## Getting Help

- Check logs: `docker-compose logs [service-name]`
- View health: `curl http://localhost:8000/health`
- Open an issue on GitHub
- Review [README.md](../README.md) for more details

## Summary

You now have a complete event-driven booking application running with:

✅ REST API for booking operations
✅ Reliable event publishing with transactional outbox pattern
✅ Event streaming via Kafka
✅ Distributed tracing with Jaeger and OpenTelemetry
✅ Metrics visualization with Grafana and Prometheus
✅ Complete observability stack

Explore, experiment, and learn! 🚀
