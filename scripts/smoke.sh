#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

# Postgres readiness (inside the container)
echo -n "Waiting for Postgres in container "
until docker compose exec -T postgres pg_isready -U app -d appdb >/dev/null 2>&1; do
  printf "."
  sleep 2
done
echo " OK"

echo "Seeding 2 engagement_events rows..."
docker compose exec -T postgres psql -U app -d appdb -c "
INSERT INTO engagement_events(content_id,user_id,event_type,event_ts,duration_ms,device,raw_payload) VALUES
('00000000-0000-0000-0000-000000000001','11111111-1111-1111-1111-111111111111','play',   now(),  60000,'web','{}'),
('00000000-0000-0000-0000-000000000001','22222222-2222-2222-2222-222222222222','finish', now(), 180000,'ios','{}');
" >/dev/null

echo "Reading 1 message from Debezium topic..."
# NOTE: using localhost:9092 because rpk runs *inside* the redpanda container
docker compose exec -T redpanda rpk topic consume cdc.public.engagement_events --brokers localhost:9092 -n 1 >/dev/null && echo "Kafka OK"

# Give the pipeline a moment to process
sleep 8

echo -n "ClickHouse rows in analytics.engagement_enriched: "
docker compose exec -T clickhouse clickhouse-client -q "SELECT count() FROM analytics.engagement_enriched" || true

echo "Redis rolling 10m leaderboard (top10m):"
docker compose exec -T redis redis-cli ZREVRANGE top10m 0 9 WITHSCORES || true
