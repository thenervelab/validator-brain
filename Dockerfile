FROM golang:1.21-alpine AS dbmate
RUN apk add --no-cache git
RUN go install github.com/amacneil/dbmate/v2@v2.6.0

FROM python:3.9-slim

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    build-essential \
    python3-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dbmate from the first stage
COPY --from=dbmate /go/bin/dbmate /usr/local/bin/dbmate

# Set working directory
WORKDIR /app

# Copy pyproject.toml
COPY pyproject.toml .

# Install Python dependencies
RUN pip install --no-cache-dir .

# Copy project files (in a specific order to ensure migrations are included)
COPY db/migrations/ /app/db/migrations/
COPY db/schema.sql /app/db/schema.sql
COPY .dbmate.yml /app/.dbmate.yml
COPY . .


# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Wait for PostgreSQL to be ready\n\
until nc -z ${POSTGRES_HOST:-localhost} ${POSTGRES_PORT:-5432}; do\n\
  echo "Waiting for PostgreSQL at ${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}..."\n\
  sleep 1\n\
done\n\
\n\
# Set up DATABASE_URL if not already set\n\
if [ -z "$DATABASE_URL" ]; then\n\
  export DATABASE_URL="postgres://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-substrate_fetcher}?sslmode=disable"\n\
  echo "Set DATABASE_URL to $DATABASE_URL"\n\
fi\n\
\n\
# Debug commands\n\
echo "Checking for migration files:"\n\
ls -la /app/db/migrations\n\
find /app -type f -name "*.sql"\n\
\n\
# Run migrations with dbmate\n\
echo "Running migrations..."\n\
cd /app && dbmate up\n\
\n\
# Start the application with uvicorn\n\
echo "Starting FastAPI application..."\n\
exec uvicorn app.main:app --host 0.0.0.0 --port 8000\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8000

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]