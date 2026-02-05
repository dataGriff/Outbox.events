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

### Create Your First Order

Using curl:
```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "CUST-001",
    "items": [
      {
        "product_id": "PROD-001",
        "quantity": 2,
        "price": 49.99
      }
    ],
    "total_amount": 99.98
  }'
```

Or use the Swagger UI at http://localhost:8000/api/docs to try it interactively.

### View the Created Order

Get the order (replace ORDER_ID with the ID from the response):
```bash
curl http://localhost:8000/api/orders/ORD-12345678
```

List all orders:
```bash
curl http://localhost:8000/api/orders
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
- Orders created per second
- Failed orders
- Events published by type
- Request duration metrics

## Step 8: Create More Orders and See the System in Action

Run multiple order creation requests:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8000/api/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customer_id\": \"CUST-$(printf %03d $i)\",
      \"items\": [{
        \"product_id\": \"PROD-001\",
        \"quantity\": $i,
        \"price\": 49.99
      }],
      \"total_amount\": $((i * 50))
    }"
  echo ""
done
```

Now observe:
- **Kafka UI**: See new events appearing
- **Grafana**: See metrics increase in real-time
- **Jaeger**: See new traces being created

## Understanding the Outbox Pattern

When you create an order, here's what happens:

1. **Order Creation**: Order is saved to MongoDB `orders` collection
2. **Event Creation**: Event is saved to MongoDB `outbox` collection (same database, atomic)
3. **Background Processing**: Outbox processor (running every 5 seconds) finds pending events
4. **Event Publishing**: Events are published to Kafka topic `business-events`
5. **Status Update**: Event status is updated to "published" in outbox collection

This ensures that events are never lost, even if Kafka is temporarily unavailable!

## Exploring MongoDB Data

Connect to MongoDB to see the data:

```bash
# Connect to MongoDB container
docker exec -it outbox-mongodb mongosh -u admin -p password123

# Switch to database
use outbox_db

# View orders
db.orders.find().pretty()

# View outbox events
db.outbox.find().pretty()

# Count pending events
db.outbox.countDocuments({status: "pending"})

# Count published events
db.outbox.countDocuments({status: "published"})

# Exit
exit
```

## Testing Order Cancellation

Cancel an order to see a different event type:

```bash
curl -X PUT http://localhost:8000/api/orders/ORD-12345678/cancel
```

Check Kafka UI to see the `order.cancelled` event!

## Viewing Prometheus Metrics Directly

1. Open http://localhost:9090
2. Click "Graph"
3. Try these queries:

```promql
# Orders created rate
rate(orders_created_total[1m])

# Total orders created
orders_created_total

# Events by type
events_published_total
```

## Next Steps

Now that you have the basics working, you can:

1. **Read the Architecture Documentation**: [docs/ARCHITECTURE.md](ARCHITECTURE.md)
2. **Explore AsyncAPI Specification**: [docs/asyncapi.yaml](asyncapi.yaml)
3. **Modify the Code**: The application uses hot-reload, so changes are reflected immediately
4. **Add New Event Types**: Extend the `EventType` enum in `src/app/models.py`
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

You now have a complete event-driven application running with:

✅ REST API for business operations
✅ Reliable event publishing with outbox pattern
✅ Event streaming via Kafka
✅ Distributed tracing with Jaeger
✅ Metrics visualization with Grafana
✅ Complete observability stack

Explore, experiment, and learn! 🚀
