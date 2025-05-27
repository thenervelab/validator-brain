#!/bin/sh

# Wait for services to be ready
echo "Waiting for services to be ready..."

# Wait for PostgreSQL
until nc -z postgres 5432; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Wait for RabbitMQ
until nc -z rabbitmq 5672; do
  echo "Waiting for RabbitMQ..."
  sleep 2
done

# If IPFS is needed (for user-profile-consumer), wait for it
if [ "$WAIT_FOR_IPFS" = "true" ]; then
  until nc -z ipfs 5001; do
    echo "Waiting for IPFS..."
    sleep 2
  done
fi

echo "All services are ready!"

# Execute the command passed to the container
exec "$@" 