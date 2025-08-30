#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

CFG="connect/debezium-postgres.json"
NAME="pg-engagement-cdc"

echo -n "Registering $NAME ... "
code=$(curl -sS -o /dev/null -w "%{http_code}" -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @"$CFG" || true)

if [[ "$code" == "201" || "$code" == "200" ]]; then
  echo "created."
elif [[ "$code" == "409" ]]; then
  echo "already exists."
else
  echo "FAILED (HTTP $code)"; 
  echo "Response:"
  curl -sS -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' --data @"$CFG" || true
  exit 1
fi

# Print status + wait until the task shows up RUNNING
echo -n "Waiting for $NAME task to be RUNNING "
for i in {1..60}; do
  status=$(curl -sS http://localhost:8083/connectors/$NAME/status || true)
  echo "$status" | grep -q '"state":"RUNNING"' && { echo "OK"; break; }
  printf "."
  sleep 2
  [[ $i -eq 60 ]] && { echo " TIMEOUT"; echo "$status"; exit 1; }
done
