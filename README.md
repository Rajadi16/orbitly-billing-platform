# Orbitly Billing Platform

Event-driven SaaS billing backend built with **Spring Boot 3**, **Kafka**, **JWT auth**, and **Stripe** (test mode).

## Architecture

Orbitly exposes a secured REST API (JWT Bearer tokens) that accepts Stripe webhook events, publishes them as `billing-events` messages on a single Kafka topic, and processes them via an idempotent Kafka consumer that persists state to PostgreSQL — all services run in Docker with no local JDK or Kafka installation required.

## Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21 (inside Docker) |
| Framework | Spring Boot 3.3 |
| Auth | Spring Security + JJWT |
| Messaging | Apache Kafka 3.7 (KRaft, no Zookeeper) |
| Payments | Stripe Java SDK (test mode) |
| Database | PostgreSQL 16 |
| Migrations | Flyway |
| Build | Maven (multi-stage Docker build) |

## Local Setup

### Prerequisites
- Docker Desktop (that's it — no Java or Kafka needed locally)

### Steps

```bash
# 1. Clone
git clone https://github.com/Rajadi16/orbitly-billing-platform.git
cd orbitly-billing-platform

# 2. Configure secrets
cp .env.example .env
# Edit .env — fill in JWT_SECRET, STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET

# 3. Start everything (postgres + kafka + app)
docker compose up --build

# App is live at http://localhost:8080
```

### Useful commands

```bash
# Rebuild only the app after code changes
docker compose up --build app

# Tail logs
docker compose logs -f app

# Tear down (keeps volumes)
docker compose down

# Tear down + wipe volumes
docker compose down -v
```

## Project Structure

```
src/
└── main/
    ├── java/com/orbitly/
    │   └── OrbitlyApplication.java
    └── resources/
        ├── application.yml
        └── db/migration/
            └── V1__init_schema.sql
Dockerfile
docker-compose.yml
.env.example
```

## Roadmap (session-by-session)

- [x] `0` — Repo scaffold, Docker Compose, README
- [ ] `1` — JWT auth (register / login endpoints)
- [ ] `2` — Stripe webhook ingestion → Kafka producer
- [ ] `3` — Idempotent Kafka consumer → billing_events table
- [ ] `4` — Billing summary REST endpoint
- [ ] `5` — Integration tests + CI
