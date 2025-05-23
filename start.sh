#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until nc -z ${POSTGRES_HOST:-localhost} ${POSTGRES_PORT:-5432}; do
  echo "Waiting for PostgreSQL at ${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}..."
  sleep 1
done

# Set up DATABASE_URL if not already set
if [ -z "$DATABASE_URL" ]; then
  export DATABASE_URL="postgres://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-substrate_fetcher}?sslmode=disable"
  echo "Set DATABASE_URL to $DATABASE_URL"
fi

echo "Checking for migration files:"
ls -la /app/db/migrations
find /app -type f -name "*.sql"

echo "Running migrations..."
cd /app && dbmate up

exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload