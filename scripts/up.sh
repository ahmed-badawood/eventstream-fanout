#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

echo "Starting stack..."
docker compose up -d

# Wait for Kafka Connect HTTP to be ready
echo -n "Waiting for Kafka Connect at http://localhost:8083 "
until curl -fsS http://localhost:8083/ >/dev/null 2>&1; do
  printf "."
  sleep 2
done
echo " OK"

./scripts/register_connector.sh
./scripts/smoke.sh
