#!/usr/bin/env bash

set -e

cleanup() {
    docker compose -f docker-compose.test.yml down -v
}

trap cleanup EXIT

docker compose -f docker-compose.test.yml up -d --wait

DB_HOST=localhost \
DB_PORT=5433 \
DB_NAME=mouse_analytics_test \
DB_USER=test_user \
DB_PASSWORD=test_password \
KAFKA_BOOTSTRAP_SERVERS=localhost:9093 \
KAFKA_TOPIC=mouse-events-test \
python -m pytest tests/test_integration -v