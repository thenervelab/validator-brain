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

RUN pip install --no-cache-dir . uvicorn[standard] watchfiles

# Copy project files (in a specific order to ensure migrations are included)
COPY db/migrations/ /app/db/migrations/
COPY db/schema.sql /app/db/schema.sql
COPY .dbmate.yml /app/.dbmate.yml
COPY . .


# Copy the entrypoint script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Expose port
EXPOSE 8000

# Set entrypoint
ENTRYPOINT ["/app/start.sh"]