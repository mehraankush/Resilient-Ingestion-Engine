# Resilient Ingestion Engine

A FastAPI service for ingesting ticket events safely. It uses Redis for
idempotency and quarantines invalid payloads in a dead-letter queue (DLQ) for
inspection and replay.

## Features

- Idempotent event ingestion with client-provided or generated keys
- Protection against concurrent duplicate processing
- Cached responses for completed requests
- Validation of supported ticket event types
- DLQ storage for malformed or invalid payloads
- Repair and replay workflow for quarantined payloads
- Redis health reporting

## Project structure

```text
.
├── config/          # FastAPI lifespan and Redis startup
├── dlq/             # Dead-letter queue models, routes, and service
├── domain/          # Ingestion use case and domain results/errors
├── main.py          # FastAPI application and ingestion endpoint
├── redis_client.py  # Shared Redis connection manager
└── schema.py        # Ticket payload validation schema
```

## Run locally

Requirements: Python 3.11+, Docker, and Docker Compose.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d redis
uvicorn main:app --reload
```

The API is available at `http://localhost:8000`. Interactive documentation is
available at `http://localhost:8000/docs`.

## API

### Health check

```http
GET /healthz
```

### Ingest an event

```http
POST /api/v1/ingest
Idempotency-Key: optional-client-key
Content-Type: application/json
```

```json
{
  "event_id": "evt_9872a3b1",
  "tenant_id": "org_corp_42",
  "event_type": "ticket.created",
  "timestamp": "2026-10-11T12:00:00Z",
  "data": {
    "ticket_id": "TCK-1049",
    "subject": "VPN connection dropping intermittently",
    "priority": "P2"
  }
}
```

Supported event types are `ticket.created`, `ticket.updated`, and
`ticket.resolved`. Invalid payloads receive `202 Accepted` and are placed in
the DLQ.

### List quarantined payloads

```http
GET /api/v1/dlq?limit=20
```

### Replay a quarantined payload

Replay the original payload:

```http
POST /api/v1/dlq/{quarantine_id}/replay
Content-Type: application/json

{}
```

Or provide a corrected payload:

```json
{
  "repaired_payload": {
    "event_id": "evt_9872a3b1",
    "tenant_id": "org_corp_42",
    "event_type": "ticket.created",
    "timestamp": "2026-10-11T12:00:00Z",
    "data": {}
  }
}
```

## Idempotency behavior

- The `Idempotency-Key` header is used when supplied.
- Otherwise, the service derives a key from `tenant_id` and `event_id`.
- A request already being processed returns `409 Conflict`.
- A completed duplicate returns the cached result with `X-Cache: HIT`.
- A new request returns `201 Created` with `X-Cache: MISS`.
