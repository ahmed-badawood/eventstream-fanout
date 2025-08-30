# Eventstream Fanout

Change Data Capture from **Postgres → Debezium → Redpanda (Kafka)**, processed by a **Spark Structured Streaming** job that writes:
- a rolling **leaderboard** to **Redis**
- **enriched analytics** rows to **ClickHouse**

Built for easy “one-command demo” on any Docker host (your GCP VM is perfect).

---

## Prerequisites

- Docker + Docker Compose v2 (`docker compose ...`)
- `curl`
- (Optional) `jq`, `rpk` (Redpanda CLI) — we call `rpk` inside the Redpanda container so you don’t need it locally.

> Default creds are for demo only: Postgres/ClickHouse/Redis users are `app/app`.

---

## Quick start (5 minutes)

```bash
# 1) Start everything
docker compose up -d

# 2) Register the Debezium connector (creates Kafka topics & starts CDC)
./scripts/register_connector.sh

# 3) Seed a content row and a couple of engagement events, then smoke-test outputs
./scripts/smoke.sh
