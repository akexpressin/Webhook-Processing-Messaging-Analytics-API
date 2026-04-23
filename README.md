# Webhook Processing & Messaging Analytics API

This project is a containerized FastAPI service for ingesting WhatsApp-like
messages with idempotency, HMAC verification, and basic analytics.

---

## How to Run

### Prerequisites
- Docker Desktop

### Start the service

docker compose up --build

The API will be available at:

http://localhost:8000

### Stop the service

docker compose down -v

---

## Environment Variables

WEBHOOK_SECRET  
Secret key used for HMAC-SHA256 verification.

DATABASE_URL  
SQLite database path (default: sqlite:////data/app.db)

---

## API Endpoints

### Health Endpoints
- GET /health/live  
- GET /health/ready  

### Webhook
- POST /webhook  

Validates the X-Signature header using HMAC-SHA256 over the raw request body.
The endpoint is idempotent using message_id as the primary key.

### Messages
- GET /messages  

Supports pagination and filters:
- limit
- offset
- from
- since
- q

### Stats
- GET /stats  

Returns total messages, sender counts, top senders, and first/last timestamps.

---

## Design Decisions

HMAC Verification  
The raw request body is read once and verified before parsing to ensure
correct signature validation.

Idempotency  
Enforced via a SQLite primary key on message_id with graceful handling of
duplicate inserts.

Pagination  
Implemented using limit and offset with a separate total count query so that
the total value is independent of pagination.

Storage  
SQLite is used as the database and stored in a Docker volume to ensure
persistence across container restarts.

---

## Setup Used

VS Code, Postman, Docker Desktop, and ChatGPT for guidance.
