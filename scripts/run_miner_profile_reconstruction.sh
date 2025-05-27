#!/bin/bash

set -e

echo "🔄 Running Miner Profile Reconstruction Processor..."

# Check if minikube is running
if ! minikube status > /dev/null 2>&1; then
    echo "❌ Minikube is not running. Please start it with: minikube start"
    exit 1
fi

# Run the processor as a one-time job
kubectl run miner-profile-reconstruction-processor \
    --image=ipfs-service-validator:latest \
    --rm -i --restart=Never \
    --env="DATABASE_URL=postgres://user:password@postgres-service:5432/substrate_fetcher?sslmode=disable" \
    --env="RABBITMQ_URL=amqp://admin:admin@rabbitmq-service:5672/" \
    --env="PYTHONUNBUFFERED=1" \
    -- python rabbitmq/miner_profile_reconstruction_processor.py

echo "✅ Miner profile reconstruction processor completed!" 