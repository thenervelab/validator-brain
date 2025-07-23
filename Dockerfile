FROM golang:1.21-alpine AS dbmate
RUN apk add --no-cache git
RUN go install github.com/amacneil/dbmate/v2@v2.6.0

# Build stage - install dependencies
FROM python:3.9-slim AS builder

# Set environment variables for build
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for better caching
COPY requirements.txt /tmp/
WORKDIR /tmp

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && find /usr/local -type d -name __pycache__ -exec rm -rf {} + \
    && find /usr/local -type f -name "*.pyc" -delete

# Runtime stage - minimal image
FROM python:3.9-slim AS runtime

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONPATH=/app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && apt-get autoremove -y

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy dbmate from the first stage
COPY --from=dbmate /go/bin/dbmate /usr/local/bin/dbmate

# Set working directory
WORKDIR /app

# Copy only necessary application files
COPY app/ ./app/
COPY rabbitmq/ ./rabbitmq/
COPY substrate_fetcher/ ./substrate_fetcher/
COPY db/ ./db/
COPY *.py ./
COPY start.sh ./start.sh

# Make start script executable
RUN chmod +x /app/start.sh

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Set entrypoint
ENTRYPOINT ["/app/start.sh"]