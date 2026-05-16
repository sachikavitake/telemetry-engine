# Telemetry Ingestion Engine

A high-performance telemetry ingestion pipeline built with Go, NATS JetStream, and DuckDB.

## Architecture

```
┌──────────┐         ┌──────────────────┐         ┌─────────────────────┐
│  Clients │──POST──→│  API Server      │──pub──→  │    NATS JetStream   │
│          │         │  :8090           │         │    :4222            │
│          │←─429────│  - rate limiting  │         │                     │
│          │←─503────│  - backpressure  │←─lag────│                     │
└──────────┘         └──────────────────┘         └────────┬────────────┘
                                                           │ pull (batches)
                                                           ▼
                                                  ┌─────────────────────┐
                                                  │  Worker             │
                                                  │  - Batch writer     │
                                                  │  - Dead letter queue│
                                                  │  - Query API :8091  │
                                                  │  - DuckDB storage   │
                                                  └─────────────────────┘
```

### Request Flow

```
Request → Rate Limit (429) → Backpressure (503) → Publish to NATS (202)
```

## Components

### API Server (`main.go`)
- Receives telemetry events via `POST /ingest`
- Per-IP rate limiting with token bucket algorithm
- Backpressure detection via consumer lag monitoring
- Publishes events to NATS JetStream for durable buffering
- Decoupled from storage — stays fast even if the database is slow

### Worker (`worker/main.go`)
- Consumes events from NATS in batches (up to 100 events or 5s timeout)
- Writes batches to DuckDB using transactions
- Acknowledges messages only after successful write (at-least-once delivery)
- Serves a query API on port 8091

### Rate Limiting
- Per-IP token bucket rate limiter (100 req/s per client IP)
- Proxy-aware IP extraction (`X-Forwarded-For` → `X-Real-IP` → `RemoteAddr`)
- Returns `429 Too Many Requests` with `Retry-After: 1` header when exceeded
- Automatic cleanup of stale entries every 10 minutes

### Backpressure Handling
- API monitors consumer lag and rejects new events with HTTP 503 when the worker falls behind
- Returns `Retry-After: 5` header so clients know when to retry
- Automatically resumes when the queue drains

### Dead Letter Queue
- Failed messages are retried up to 3 times before being routed to a dead letter queue
- Preserves original data and error reason for debugging
- Stored in both NATS (`TELEMETRY.deadletter`) and DuckDB (`dead_letters` table)
- Queryable via `GET /query/dlq`

### Query API (part of Worker)
- `GET /query/stats` — P50, P95, P99 latency, min, max, avg
- `GET /query/stats?service=auth-service` — filter by service
- `GET /query/stats?window=1h` — filter by time window
- `GET /query/services` — per-service breakdown with error rates
- `GET /query/dlq` — inspect dead-lettered messages

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Ingestion API | Go `net/http` | No framework needed, production-grade stdlib |
| Message Buffer | NATS JetStream | Durable streaming with low overhead (~30MB RAM) |
| Storage | DuckDB | Columnar OLAP — sub-second P95/P99 queries |
| Rate Limiting | `golang.org/x/time/rate` | Token bucket algorithm, concurrent-safe |

## Event Schema

```json
{
  "service": "auth-service",
  "endpoint": "/login",
  "status_code": 200,
  "latency_ms": 45.2,
  "timestamp": "2026-05-11T12:00:00Z"
}
```

## Error Responses

| Status | Meaning | When |
|--------|---------|------|
| `429 Too Many Requests` | Rate limit exceeded | Client IP exceeds 100 req/s |
| `503 Service Unavailable` | Backpressure active | Worker is behind by 5000+ messages |
| `400 Bad Request` | Invalid JSON | Malformed request body |
| `405 Method Not Allowed` | Wrong HTTP method | Anything other than POST |

## Sample Query Response

```
GET /query/services

[
  {
    "service": "auth-service",
    "total_events": 10,
    "avg_latency_ms": 101.6,
    "p95_latency_ms": 177.7,
    "error_rate_pct": 0
  },
  {
    "service": "payment-service",
    "total_events": 5,
    "avg_latency_ms": 391.8,
    "p95_latency_ms": 511.4,
    "error_rate_pct": 20
  }
]
```

## Running with Docker Compose

### Prerequisites
- Docker

### Start Everything
```bash
docker compose up --build
```

This starts NATS, the API server, and the worker. Data persists across restarts.

```bash
# Stop (keep data)
docker compose down

# Stop and wipe data
docker compose down -v
```

## Running Locally (without Docker)

### Prerequisites
- Go 1.21+
- Docker (for NATS)

### Start NATS
```bash
docker run -d --name nats-server -p 4222:4222 -p 8222:8222 nats:latest -js -m 8222
```

### Start API Server
```bash
go run main.go
```

### Start Worker
```bash
cd worker && go run main.go
```

## Usage

### Send Events
```bash
curl -X POST http://localhost:8090/ingest \
  -H "Content-Type: application/json" \
  -d '{"service": "auth-service", "endpoint": "/login", "status_code": 200, "latency_ms": 45.2}'
```

### Query Analytics
```bash
curl http://localhost:8091/query/stats
curl http://localhost:8091/query/services
curl "http://localhost:8091/query/stats?service=auth-service&window=1h"
```
