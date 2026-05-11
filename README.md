# Telemetry Ingestion Engine

A high-performance telemetry ingestion pipeline built with Go, NATS JetStream, and DuckDB.

## Architecture

```
┌──────────┐         ┌──────────────┐         ┌─────────────────────┐
│  Clients │──POST──→│  API Server  │──pub──→  │    NATS JetStream   │
│  (curl)  │         │  :8090       │         │    :4222            │
└──────────┘         └──────────────┘         └────────┬────────────┘
                                                       │ pull (batches)
                                                       ▼
                                              ┌─────────────────────┐
                                              │  Worker             │
                                              │  - Batch writer     │
                                              │  - Query API :8091  │
                                              │  - DuckDB storage   │
                                              └─────────────────────┘
```

## Components

### API Server (`main.go`)
- Receives telemetry events via `POST /ingest`
- Publishes events to NATS JetStream for durable buffering
- Decoupled from storage — stays fast even if the database is slow

### Worker (`worker/main.go`)
- Consumes events from NATS in batches (up to 100 events or 5s timeout)
- Writes batches to DuckDB using transactions
- Acknowledges messages only after successful write (at-least-once delivery)
- Serves a query API on port 8091

### Query API (part of Worker)
- `GET /query/stats` — P50, P95, P99 latency, min, max, avg
- `GET /query/stats?service=auth-service` — filter by service
- `GET /query/stats?window=1h` — filter by time window
- `GET /query/services` — per-service breakdown with error rates

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Ingestion API | Go `net/http` | No framework needed, production-grade stdlib |
| Message Buffer | NATS JetStream | Durable streaming with low overhead (~30MB RAM) |
| Storage | DuckDB | Columnar OLAP — sub-second P95/P99 queries |

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

## Running Locally

### Prerequisites
- Go 1.21+
- Docker

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
